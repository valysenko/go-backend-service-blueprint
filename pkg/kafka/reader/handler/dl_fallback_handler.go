package handler

import (
	"context"
	"errors"
	"fmt"
	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/kafka/writer"
	"log/slog"

	kafkaPkg "github.com/segmentio/kafka-go"
)

type DlIncrementStorage interface {
	GetDlCounter(ctx context.Context, msg *reader.Message) (int, error)
	IncrementCounter(ctx context.Context, msg *reader.Message) error
	DecrementCounter(ctx context.Context, msg *reader.Message) error
}

// DLFallbackMessageHandler is the BaseFlow DL router. On inner failure — or when
// the key already has DL count > 0 — it writes the message to dlTopic and
// increments the per-key counter so per-key ordering is preserved.
//
// DiscardError skips DL and acks.
// ReaderFailure propagates.
type DLFallbackMessageHandler struct {
	inner   reader.MessageHandler
	writer  writer.Writer
	storage DlIncrementStorage
	logger  *slog.Logger
	dlTopic string
}

func NewDLFallbackMessageHandler(inner reader.MessageHandler, storage DlIncrementStorage, writer writer.Writer, logger *slog.Logger, dlTopic string) *DLFallbackMessageHandler {
	return &DLFallbackMessageHandler{
		inner:   inner,
		storage: storage,
		writer:  writer,
		logger:  logger,
		dlTopic: dlTopic,
	}
}

func (h *DLFallbackMessageHandler) Handle(ctx context.Context, msg *reader.Message) error {
	logAttrs := []any{
		"topic", msg.Topic(),
		"key", string(msg.Key()),
		"partition", msg.Partition(),
		"offset", msg.Offset(),
	}

	count, err := h.storage.GetDlCounter(ctx, msg)
	if err != nil {
		return reader.NewReaderFailure(err, fmt.Sprintf(
			"fetch dl counter for topic %q key %q partition %d offset %d",
			msg.Topic(), string(msg.Key()), msg.Partition(), msg.Offset()))
	}

	if count > 0 {
		reason := "there are messages for this partition key in DL"
		return h.writeToDlTopic(ctx, msg, logAttrs, count, reason)
	}

	handlingErr := h.inner.Handle(ctx, msg)
	if handlingErr == nil {
		return nil
	}

	var discardErr *reader.DiscardError
	if errors.As(handlingErr, &discardErr) {
		h.logger.InfoContext(ctx, "discard error, skipping DL",
			append(logAttrs, "error", handlingErr.Error())...)
		return nil
	}

	// ReaderFailure means "infra is broken, do not absorb" — propagate to reader.
	var rf *reader.ReaderFailure
	if errors.As(handlingErr, &rf) {
		return handlingErr
	}

	return h.writeToDlTopic(ctx, msg, logAttrs, count, handlingErr.Error())
}

func (h *DLFallbackMessageHandler) writeToDlTopic(ctx context.Context, msg *reader.Message, logAttrs []any, prevCount int, reason string) error {
	// Reserve the counter before publishing: a DL consumer may process the
	// message as soon as the broker acknowledges it.
	if err := h.storage.IncrementCounter(ctx, msg); err != nil {
		return reader.NewReaderFailure(err, fmt.Sprintf(
			"increment dl counter for topic %q key %q partition %d offset %d",
			msg.Topic(), string(msg.Key()), msg.Partition(), msg.Offset()))
	}

	if pubErr := h.writer.WriteMessages(ctx, kafkaPkg.Message{
		Key:     msg.Key(),
		Value:   msg.Body(),
		Headers: msg.RawMessage().Headers,
	}); pubErr != nil {
		rollbackErr := h.storage.DecrementCounter(ctx, msg)
		return reader.NewReaderFailure(errors.Join(pubErr, rollbackErr), fmt.Sprintf(
			"publish to dl topic %q key %q partition %d offset %d",
			h.dlTopic, string(msg.Key()), msg.Partition(), msg.Offset()))
	}

	h.logger.InfoContext(ctx, "message moved to DL",
		append(logAttrs, "dl", h.dlTopic, "prev_dl_count", prevCount, "reason", reason)...)

	return nil
}
