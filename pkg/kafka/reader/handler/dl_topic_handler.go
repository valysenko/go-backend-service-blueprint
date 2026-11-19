package handler

import (
	"context"
	"errors"
	"fmt"
	"go-backend-service-blueprint/pkg/kafka/reader"
	"log/slog"
	"time"
)

type DlDecrementStorage interface {
	DecrementCounter(ctx context.Context, msg *reader.Message) error
}

// DLTopicMessageHandler consumes messages from the DL topic and runs the inner handler in-place, retrying
// transient errors up to maxAttempts (0 = retry forever).
//
// On success, DiscardError, or retry exhaustion the DL counter is decremented
// so BaseFlow can resume the key.
// ReaderFailure propagates.
type DLTopicMessageHandler struct {
	inner   reader.MessageHandler
	storage DlDecrementStorage
	logger  *slog.Logger
	// 0 = retry forever
	// >0 = give maxAttempts and then log and skip
	maxAttempts int
}

func NewDLTopicMessageHandler(
	inner reader.MessageHandler,
	storage DlDecrementStorage,
	logger *slog.Logger,
	maxAttempts int,
) *DLTopicMessageHandler {
	return &DLTopicMessageHandler{
		inner:       inner,
		storage:     storage,
		logger:      logger,
		maxAttempts: maxAttempts,
	}
}

func (h *DLTopicMessageHandler) Handle(ctx context.Context, msg *reader.Message) error {
	logAttrs := []any{
		"topic", msg.Topic(),
		"key", string(msg.Key()),
		"partition", msg.Partition(),
		"offset", msg.Offset(),
	}

	// dl retries loop
	attempts := 0
	for {
		err := h.inner.Handle(ctx, msg)
		if err == nil {
			break
		}

		var discardErr *reader.DiscardError
		if errors.As(err, &discardErr) {
			// message skipped and committed
			h.logger.WarnContext(ctx, "dl message discarded, decrementing counter and committing",
				append(logAttrs, "attempts", attempts+1, "error", err)...)
			if decErr := h.storage.DecrementCounter(ctx, msg); decErr != nil {
				return reader.NewReaderFailure(decErr, fmt.Sprintf(
					"decrement dl counter after discard for topic %q key %q",
					msg.Topic(), string(msg.Key())))
			}
			return nil
		}

		// ReaderFailure means "infra is broken, do not absorb" - propagate to reader
		var rf *reader.ReaderFailure
		if errors.As(err, &rf) {
			return err
		}

		attempts++
		h.logger.DebugContext(ctx, "dl processing attempt failed",
			append(logAttrs, "attempt", attempts, "error", err)...)

		if h.maxAttempts > 0 && attempts >= h.maxAttempts {
			// message skipped and committed
			h.logger.WarnContext(ctx, "dl retries exhausted, decrementing counter and committing",
				append(logAttrs, "attempts", attempts, "last_error", err)...)
			if decErr := h.storage.DecrementCounter(ctx, msg); decErr != nil {
				return reader.NewReaderFailure(decErr, fmt.Sprintf(
					"decrement dl counter after retries for topic %q key %q",
					msg.Topic(), string(msg.Key())))
			}
			return nil
		}

		select {
		case <-ctx.Done():
			h.logger.WarnContext(ctx, "dl retry loop cancelled",
				append(logAttrs, "attempts", attempts, "last_error", err, "ctx_err", ctx.Err())...)
			return ctx.Err()
		// TODO: keep a fixed 1s delay for simplicity; with maxAttempts==0 (retry forever)
		case <-time.After(1 * time.Second):
		}
	}

	if err := h.storage.DecrementCounter(ctx, msg); err != nil {
		return reader.NewReaderFailure(err, fmt.Sprintf(
			"decrement dl counter after success for topic %q key %q",
			msg.Topic(), string(msg.Key())))
	}
	h.logger.InfoContext(ctx, "processed message and decremented DL counter",
		append(logAttrs, "attempts", attempts)...)

	return nil
}
