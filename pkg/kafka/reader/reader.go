package reader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"sync"

	"go-backend-service-blueprint/pkg/ctxutils"
	"go-backend-service-blueprint/pkg/kafka/config"
	"go-backend-service-blueprint/pkg/kafka/helper"

	kafkaPkg "github.com/segmentio/kafka-go"
)

type MessageHandler interface {
	Handle(ctx context.Context, m *Message) error
}

type KafkaClient interface {
	FetchMessage(ctx context.Context) (kafkaPkg.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkaPkg.Message) error
	Close() error
}

func NewKafkaClient(
	cfg *config.KafkaConfig,
	logger *slog.Logger,
	topicName string,
	consumerGroup string,
) (KafkaClient, error) {
	if err := helper.CheckTopicExists(cfg.Brokers, topicName, logger); err != nil {
		return nil, err
	}

	return kafkaPkg.NewReader(kafkaPkg.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  consumerGroup,
		Topic:    topicName,
		MinBytes: 1,               // returns immediately. increase if throughput is preferred over latency
		MaxBytes: 10e6,            // default value is used - 10MB.
		MaxWait:  1 * time.Second, // idle poll interval only (MinBytes=1 delivers messages immediately);
		// bounds how long a blocked fetch lingers on shutdown, so keep it low for fast graceful stop.
		ErrorLogger: kafkaPkg.LoggerFunc(func(s string, i ...interface{}) {
			logger.Error(fmt.Sprintf(s, i...))
		}),
	}), nil
}

// single message reader without batching messages
type KafkaTopicReader struct {
	client  KafkaClient
	logger  *slog.Logger
	handler MessageHandler
	closers []io.Closer

	closeOnce sync.Once
}

func NewKafkaTopicReader(
	client KafkaClient,
	h MessageHandler,
	logger *slog.Logger,
	closers ...io.Closer,
) *KafkaTopicReader {
	return &KafkaTopicReader{
		client:  client,
		handler: h,
		logger:  logger,
		closers: closers,
	}
}

// Close releases the kafka client plus every resource whose lifetime is bound to
// this reader (the BaseFlow DL writer, for instance). Run calls it on exit, so
// the only other caller is one that built a reader it will never Run — a later
// reader in the set failed to construct. Safe to call more than once.
func (r *KafkaTopicReader) Close() error {
	var err error
	r.closeOnce.Do(func() {
		var errs []error
		if cerr := r.client.Close(); cerr != nil {
			errs = append(errs, fmt.Errorf("close kafka client: %w", cerr))
		}
		for _, c := range r.closers {
			if cerr := c.Close(); cerr != nil {
				errs = append(errs, fmt.Errorf("close attached resource: %w", cerr))
			}
		}
		err = errors.Join(errs...)
	})
	return err
}

// TODO: test in next PR
// TODO: investigate if ok
// {"time":"2026-04-23T06:28:05.663900924Z","level":"ERROR","msg":"Failed to sync group user-management-group: [27] Rebalance In Progress: the coordinator has begun rebalancing the group, the client should rejoin the group"}
// {"time":"2026-04-23T06:28:05.663964091Z","level":"ERROR","msg":"[27] Rebalance In Progress: the coordinator has begun rebalancing the group, the client should rejoin the group"}
func (r *KafkaTopicReader) Run(ctx context.Context) error {
	msgCh := make(chan *Message, 16)
	errCh := make(chan error, 1)

	fetchCtx, fetchCancel := context.WithCancel(ctx)

	// Cleanup resources:
	// Reader owns kafka reader connection
	// Handlers own kafka writers
	defer func() {
		fetchCancel()
		r.logger.Info("shutting down Kafka consumer...")
		if err := r.Close(); err != nil {
			r.logger.Error("error closing reader resources", "error", err)
		}
		r.logger.Info("shutdown complete.")
	}()

	go func() {
		for {
			m, err := r.client.FetchMessage(fetchCtx)
			if err != nil {
				errCh <- err
				return
			}

			// select prevents blocking on msgCh <- NewMessage(m) if buffer if full
			select {
			case msgCh <- NewMessage(m):
			case <-fetchCtx.Done():
				return
			}
		}
	}()

	var terminalErr error

run:
	for {
		// TODO: high log volume for received/handled/commit. ok for start
		select {
		case <-ctx.Done():
			r.logger.Info("context canceled, stopping consumer")
			break run

		case msg := <-msgCh:
			// select may pick this branch even after ctx is canceled if msgCh is buffered;
			// re-check to avoid running the handler with a canceled context
			if ctx.Err() != nil {
				r.logger.Info("context canceled, stopping consumer")
				break run
			}

			requestID, _ := msg.GetHeader(ctxutils.RequestID)
			msgCtx := ctxutils.WithRequestID(ctx, requestID)

			r.logger.InfoContext(msgCtx, "received message", "key", string(msg.Key()), "topic", msg.Topic(), "partition", msg.Partition(), "offset", msg.Offset())

			// handlers handle retries, publish to dl. if for some reason handler returned an error - reader returns an error and pod restarts.
			// Three classes of escape:
			//   - ctx.Canceled / DeadlineExceeded: graceful shutdown, info log.
			//   - *ReaderFailure: expected fatal escape — handler infra blip (Redis, DL writer)
			//     OR processor signaling a fully unreachable dependency. Error log.
			//   - anything else: bug — an error type that should have been handled by the
			//     chain. Investigate where it escaped and fix the missing absorption (or
			//     tag it as ReaderFailure if it's genuinely a fatal infra error).
			if err := r.handler.Handle(msgCtx, msg); err != nil {
				var rf *ReaderFailure
				switch {
				case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
					r.logger.InfoContext(msgCtx, "handler returned ctx error, stopping consumer", "error", err)
				case errors.As(err, &rf):
					r.logger.ErrorContext(msgCtx, "reader-fatal infra failure, pod will restart", "error", err)
				default:
					r.logger.ErrorContext(msgCtx, "unexpected error escaped handler chain, pod will restart",
						"error", err, "error_type", fmt.Sprintf("%T", err))
				}
				return err
			}
			r.logger.InfoContext(msgCtx, "handled message", "key", string(msg.Key()), "topic", msg.Topic(), "partition", msg.Partition(), "offset", msg.Offset())

			if err := r.client.CommitMessages(msgCtx, msg.RawMessage()); err != nil {
				// after pod restarts, reader gets this message again. DeduplicationMessageHandler should skip processing and then commits
				return fmt.Errorf("commit message at offset %d on topic %q: %w", msg.Offset(), msg.Topic(), err)
			}
			r.logger.InfoContext(msgCtx, "committed message", "key", string(msg.Key()), "topic", msg.Topic(), "partition", msg.Partition(), "offset", msg.Offset())

		case err := <-errCh:
			if errors.Is(err, context.Canceled) {
				r.logger.Info("context canceled, stopping consumer")
			} else if errors.Is(err, io.EOF) {
				r.logger.Info("no more messages to consume, stopping consumer")
			} else {
				r.logger.Error("unable to read message from kafka", "error", err)
				terminalErr = err
			}
			break run
		}
	}

	return terminalErr
}
