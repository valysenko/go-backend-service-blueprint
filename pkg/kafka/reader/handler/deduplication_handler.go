package handler

import (
	"context"
	"fmt"
	"go-backend-service-blueprint/pkg/kafka/reader"
)

// package has Redis implementation for tracking processed messages
// possible for the package client to implement and inject own repo
type DeduplicationStorage interface {
	IsAcked(ctx context.Context, msg *reader.Message) (bool, error)
	Ack(ctx context.Context, msg *reader.Message) error
}

// DeduplicationMessageHandler is the top of the processing chain for both flows: BaseFlow and DLFlow.
// it skips processing of already processed messages
type DeduplicationMessageHandler struct {
	storage      DeduplicationStorage
	innerHandler reader.MessageHandler
}

func NewDeduplicationMessageHandler(storage DeduplicationStorage, innerHandler reader.MessageHandler) *DeduplicationMessageHandler {
	return &DeduplicationMessageHandler{
		storage:      storage,
		innerHandler: innerHandler,
	}
}

func (h *DeduplicationMessageHandler) Handle(ctx context.Context, msg *reader.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	acked, err := h.storage.IsAcked(ctx, msg)
	if err != nil {
		return reader.NewReaderFailure(err, fmt.Sprintf("deduplication IsAcked for topic %q key %q",
			msg.Topic(), string(msg.Key())))
	}
	if acked {
		return nil
	}

	// inner errors are already typed by their handler
	if err := h.innerHandler.Handle(ctx, msg); err != nil {
		return err
	}

	if err := h.storage.Ack(ctx, msg); err != nil {
		return reader.NewReaderFailure(err, fmt.Sprintf("deduplication Ack for topic %q key %q",
			msg.Topic(), string(msg.Key())))
	}
	return nil
}
