package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go-backend-service-blueprint/pkg/ctxutils"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Message struct {
	ID   string
	Body []byte
}

const redialInterval = 5 * time.Second

type Publisher struct {
	cfg  Config
	spec QueueSpec

	mu       sync.Mutex // for serializable operations
	conn     *amqp.Connection
	ch       *amqp.Channel
	returns  chan amqp.Return
	nextDial time.Time
}

func NewPublisher(cfg Config, spec QueueSpec) *Publisher {
	return &Publisher{cfg: cfg, spec: spec}
}

func (p *Publisher) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.ensureChannel()
}

func (p *Publisher) Name() string {
	return "rabbitmq_publisher_" + p.spec.Name
}

func (p *Publisher) Ping() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil || p.conn.IsClosed() {
		return ErrNotConnected
	}

	return nil
}

func (p *Publisher) Publish(ctx context.Context, msg Message) error {
	return p.publish(ctx, msg, 0)
}

func (p *Publisher) PublishWithDelay(ctx context.Context, msg Message, delay time.Duration) error {
	if p.spec.Kind != KindDelayed {
		return errors.New("rabbitmq: delay requires a delayed queue")
	}
	if delay < time.Millisecond || delay.Milliseconds() > 2147483647 {
		return errors.New("rabbitmq: delay must be between 1ms and 2147483647ms")
	}
	return p.publish(ctx, msg, delay)
}

func (p *Publisher) publish(ctx context.Context, msg Message, delay time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.ensureChannel(); err != nil {
		return err
	}

	ch := p.ch
	if ch == nil {
		return fmt.Errorf("rabbitmq: publish to %q: no channel", p.spec.Name)
	}
	drainReturns(p.returns)

	headers := amqp.Table{ctxutils.RequestID: ctxutils.GetFromContext(ctx)}
	if delay > 0 {
		headers["x-delay"] = int32(delay.Milliseconds())
	}
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(ctx, p.spec.exchange(), p.spec.Name, p.spec.routesOnPublish(), false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    msg.ID,
		Timestamp:    time.Now(),
		Headers:      headers,
		Body:         msg.Body,
	})
	if err != nil {
		p.reset()
		return fmt.Errorf("rabbitmq: publish to %q: %w", p.spec.Name, err)
	}
	if confirmation == nil {
		return fmt.Errorf("rabbitmq: publish to %q: channel is not in confirm mode", p.spec.Name)
	}

	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		p.reset()
		return fmt.Errorf("rabbitmq: publish to %q: wait for confirm: %w", p.spec.Name, err)
	}
	if !acked {
		return fmt.Errorf("rabbitmq: publish to %q: %w", p.spec.Name, ErrPublishNacked)
	}
	if err := unroutable(p.returns); err != nil {
		return fmt.Errorf("rabbitmq: publish to %q: %w", p.spec.Name, err)
	}

	return nil
}

// unroutable reports a basic.return for the publish that was just confirmed.
// The broker sends the return before the confirm, so anything queued by now
// belongs to that publish; both callers keep a single publish in flight, which
// is what makes that correlation sound.
func unroutable(returns <-chan amqp.Return) error {
	select {
	case ret, ok := <-returns:
		if !ok {
			return nil
		}

		return fmt.Errorf("%w: %d %s", ErrPublishUnroutable, ret.ReplyCode, ret.ReplyText)
	default:
		return nil
	}
}

// drainReturns clears returns left by an abandoned publish. The channel must
// never fill up: amqp091 delivers returns from the same goroutine that reads
// the connection, so an unread one stalls the whole channel.
func drainReturns(returns <-chan amqp.Return) {
	for {
		select {
		case _, ok := <-returns:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.closeLocked()
}

func (p *Publisher) ensureChannel() error {
	if p.ch != nil && !p.ch.IsClosed() {
		return nil
	}

	p.reset()
	if time.Now().Before(p.nextDial) {
		return fmt.Errorf("rabbitmq: publish to %q: %w", p.spec.Name, ErrNotConnected)
	}

	if err := p.connect(); err != nil {
		p.nextDial = time.Now().Add(redialInterval)
		return err
	}
	p.nextDial = time.Time{}

	return nil
}

func (p *Publisher) connect() error {
	conn, err := Dial(p.cfg)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("rabbitmq: open channel: %w", err)
	}

	if err := Declare(ch, p.spec); err != nil {
		_ = conn.Close()
		return err
	}

	if err := ch.Confirm(false); err != nil {
		_ = conn.Close()
		return fmt.Errorf("rabbitmq: enable publisher confirms: %w", err)
	}

	p.conn = conn
	p.ch = ch
	p.returns = ch.NotifyReturn(make(chan amqp.Return, 1))

	return nil
}

func (p *Publisher) reset() {
	_ = p.closeLocked()
}

func (p *Publisher) closeLocked() error {
	var errs []error
	if p.ch != nil && !p.ch.IsClosed() {
		if err := p.ch.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.conn != nil && !p.conn.IsClosed() {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	p.ch = nil
	p.conn = nil
	p.returns = nil

	return errors.Join(errs...)
}
