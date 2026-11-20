package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go-backend-service-blueprint/pkg/ctxutils"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	reconnectInitialBackoff = time.Second
	reconnectMaxBackoff     = 30 * time.Second
	deathHeader             = "x-death"
	// Comfortably below the broker's 30 minute delivery acknowledgement
	// timeout, which would otherwise close the channel under us.
	defaultMaxElapsed = 10 * time.Minute
)

const (
	OutcomeProcessed    = "processed"
	OutcomeRetried      = "retried"
	OutcomeDeadLettered = "dead_lettered"
	OutcomeRequeued     = "requeued"
)

type Delivery struct {
	MessageID   string
	Body        []byte
	Attempt     int
	Redelivered bool
}

type Handler interface {
	Handle(ctx context.Context, delivery Delivery) error
}

type Observer interface {
	ObserveMessage(queue, outcome string, duration time.Duration)
}

type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	// MaxElapsed caps how long a single message may stay in flight across all
	// of its retries.
	MaxElapsed time.Duration
}

func (p RetryPolicy) backoff(attempt int) time.Duration {
	delay := p.InitialBackoff
	for i := 1; i < attempt; i++ {
		delay *= 2
		if p.MaxBackoff > 0 && delay >= p.MaxBackoff {
			return p.MaxBackoff
		}
	}

	return delay
}

// exhausted reports whether the message has run out of budget. A throttled
// handler deliberately does not spend an attempt, so MaxElapsed is the only
// thing that stops a permanently rate-limited down-stream from retrying one
// message forever while it holds a worker and a prefetch slot.
func (p RetryPolicy) exhausted(attempt int, throttled bool, elapsed, next time.Duration) bool {
	if p.MaxElapsed > 0 && elapsed+next >= p.MaxElapsed {
		return true
	}

	return !throttled && attempt >= p.MaxAttempts
}

type ConsumerConfig struct {
	Spec    QueueSpec
	Handler Handler
	Workers int
	Retry   RetryPolicy
}

// one queue
type Consumer struct {
	connCfg  Config
	cfg      ConsumerConfig
	handler  Handler
	observer Observer
	logger   *slog.Logger
	sleep    func(ctx context.Context, d time.Duration) bool

	mu   sync.Mutex
	conn *amqp.Connection
}

func NewConsumer(connCfg Config, cfg ConsumerConfig, observer Observer, logger *slog.Logger) (*Consumer, error) {
	if cfg.Handler == nil {
		return nil, errors.New("rabbitmq: consumer needs a handler")
	}
	if logger == nil {
		return nil, errors.New("rabbitmq: consumer needs a logger")
	}
	if err := cfg.Spec.validate(); err != nil {
		return nil, err
	}
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.Retry.MaxAttempts < 1 {
		cfg.Retry.MaxAttempts = 1
	}
	if cfg.Retry.MaxElapsed <= 0 {
		cfg.Retry.MaxElapsed = defaultMaxElapsed
	}

	return &Consumer{
		connCfg:  connCfg,
		cfg:      cfg,
		handler:  cfg.Handler,
		observer: observer,
		logger:   logger,
		sleep:    sleepContext,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	conn, err := Dial(c.connCfg)
	backoff := reconnectInitialBackoff
	for err != nil {
		if IsFatal(err) {
			return c.fatal(err)
		}
		c.logger.Warn("rabbitmq consumer cannot connect, retrying", "queue", c.cfg.Spec.Name, "delay", backoff.String(), "error", err)
		if !c.sleep(ctx, backoff) {
			return nil
		}
		backoff = min(backoff*2, reconnectMaxBackoff)
		conn, err = Dial(c.connCfg)
	}

	backoff = reconnectInitialBackoff
	for {
		c.setConn(conn)
		started := time.Now()
		err := c.consume(ctx, conn)
		_ = conn.Close()
		c.setConn(nil)

		if ctx.Err() != nil {
			return nil
		}
		if IsFatal(err) {
			return c.fatal(err)
		}
		if time.Since(started) > reconnectMaxBackoff {
			backoff = reconnectInitialBackoff
		}

		for {
			c.logger.Warn("rabbitmq consumer lost its connection, reconnecting", "queue", c.cfg.Spec.Name, "delay", backoff.String(), "error", err)
			if !c.sleep(ctx, backoff) {
				return nil
			}
			backoff = min(backoff*2, reconnectMaxBackoff)

			conn, err = Dial(c.connCfg)
			if err == nil {
				break
			}
			if IsFatal(err) {
				return c.fatal(err)
			}
		}
	}
}

// fatal names the queue whose consumer is giving up: a set runs several of them
// and Dial errors carry no queue of their own.
func (c *Consumer) fatal(err error) error {
	c.logger.Error("rabbitmq consumer stopping, the broker will keep refusing this configuration", "queue", c.cfg.Spec.Name, "error", err)

	return fmt.Errorf("rabbitmq: consumer %q: %w", c.cfg.Spec.Name, err)
}

func (c *Consumer) Name() string {
	return "rabbitmq_consumer_" + c.cfg.Spec.Name
}

func (c *Consumer) Ping() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil || c.conn.IsClosed() {
		return ErrNotConnected
	}

	return nil
}

func (c *Consumer) setConn(conn *amqp.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.conn = conn
}

func (c *Consumer) consume(ctx context.Context, conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbitmq: open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	// Buffered: amqp091 hands the reason over while it holds the channel lock,
	// so an unread listener would stall the shutdown it is reporting on.
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))

	if err := Declare(ch, c.cfg.Spec); err != nil {
		return err
	}
	if err := ch.Qos(c.cfg.Workers, 0, false); err != nil {
		return fmt.Errorf("rabbitmq: set prefetch: %w", err)
	}

	tag := "consumer-" + ctxutils.NewRequestID()
	deliveries, err := ch.Consume(c.cfg.Spec.Name, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbitmq: consume %q: %w", c.cfg.Spec.Name, err)
	}

	c.logger.Info("rabbitmq consumer started", "queue", c.cfg.Spec.Name, "workers", c.cfg.Workers)

	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ch.Cancel(tag, false) // stops receiving new messages from Rabbitmq
		case <-stopped: // this channel prevents goroutine leak when consuming stops and this goroutine is blocked on Done
		}
	}()

	var wg sync.WaitGroup
	for range c.cfg.Workers {
		wg.Go(func() {
			for d := range deliveries {
				if ctx.Err() != nil {
					_ = d.Nack(false, true) // will redeliver mesg to the origin queue
					continue
				}
				c.process(ctx, d)
			}
		})
	}
	wg.Wait()
	close(stopped)

	if ctx.Err() != nil {
		c.logger.Info("rabbitmq consumer stopped", "queue", c.cfg.Spec.Name)
		return nil
	}

	// amqp091 notifies close listeners before it closes the delivery channel,
	// so the reason is already waiting here. Without it every broker-side
	// close looks alike and a fatal one would be retried forever.
	if amqpErr := channelError(closed); amqpErr != nil {
		return fmt.Errorf("rabbitmq: consume %q: %w", c.cfg.Spec.Name, amqpErr)
	}

	return fmt.Errorf("rabbitmq: consume %q: %w", c.cfg.Spec.Name, ErrDeliveriesStopped)
}

func channelError(closed <-chan *amqp.Error) *amqp.Error {
	select {
	case err := <-closed:
		return err
	default:
		return nil
	}
}

func (c *Consumer) process(ctx context.Context, d amqp.Delivery) {
	ctx = ctxutils.WithRequestID(ctx, headerString(d.Headers, ctxutils.RequestID))
	started := time.Now()
	log := c.logger.With(
		"queue", c.cfg.Spec.Name,
		"message_id", d.MessageId,
		"request_id", ctxutils.GetFromContext(ctx),
		"redelivered", d.Redelivered,
	)

	redelivered := d.Redelivered || d.Headers[deathHeader] != nil
	attempt := 1
	for {
		err := c.handle(ctx, time.Since(started), Delivery{MessageID: d.MessageId, Body: d.Body, Attempt: attempt, Redelivered: redelivered})
		if err == nil {
			c.settle(log, d.Ack(false), OutcomeProcessed, started)
			return
		}

		if ctx.Err() != nil {
			log.Warn("rabbitmq message interrupted by shutdown, requeueing", "attempt", attempt, "error", err)
			c.settle(log, d.Nack(false, true), OutcomeRequeued, started)
			return
		}

		var permanent *PermanentError
		if errors.As(err, &permanent) {
			log.Error("rabbitmq message failed permanently, dead-lettering", "attempt", attempt, "error", err)
			c.settle(log, d.Nack(false, false), OutcomeDeadLettered, started)
			return
		}

		delay := c.cfg.Retry.backoff(attempt)
		var retryAfter *RetryAfterError
		throttled := errors.As(err, &retryAfter)
		if throttled {
			delay = max(delay, retryAfter.Delay)
		}

		if c.cfg.Retry.exhausted(attempt, throttled, time.Since(started), delay) {
			log.Error("rabbitmq message exhausted its retries, dead-lettering", "attempt", attempt, "throttled", throttled, "error", err)
			c.settle(log, d.Nack(false, false), OutcomeDeadLettered, started)
			return
		}

		log.Warn("rabbitmq message failed, retrying", "attempt", attempt, "throttled", throttled, "delay", delay.String(), "error", err)
		c.observe(OutcomeRetried, 0)

		if !c.sleep(ctx, delay) {
			c.settle(log, d.Nack(false, true), OutcomeRequeued, started)
			return
		}
		if !throttled {
			attempt++
		}
	}
}

// handle gives the handler whatever is left of the message budget. The caller
// keeps the outer context: a spent budget has to dead-letter, while a cancelled
// outer context has to requeue, and the two must not be confused. A handler
// that ignores its context can still overrun, which is why MaxElapsed stays
// below the broker's acknowledgement timeout.
func (c *Consumer) handle(ctx context.Context, elapsed time.Duration, delivery Delivery) error {
	if c.cfg.Retry.MaxElapsed <= 0 {
		return c.handler.Handle(ctx, delivery)
	}

	budget := c.cfg.Retry.MaxElapsed - elapsed
	if budget <= 0 {
		return fmt.Errorf("rabbitmq: the %s budget for this message is spent", c.cfg.Retry.MaxElapsed)
	}

	handleCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	return c.handler.Handle(handleCtx, delivery)
}

func (c *Consumer) settle(log *slog.Logger, err error, outcome string, started time.Time) {
	if err != nil {
		log.Error("rabbitmq could not settle message", "outcome", outcome, "error", err)
	}
	c.observe(outcome, time.Since(started))
}

func (c *Consumer) observe(outcome string, duration time.Duration) {
	if c.observer != nil {
		c.observer.ObserveMessage(c.cfg.Spec.Name, outcome, duration)
	}
}

func headerString(headers amqp.Table, key string) string {
	if value, ok := headers[key].(string); ok {
		return value
	}

	return ""
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
