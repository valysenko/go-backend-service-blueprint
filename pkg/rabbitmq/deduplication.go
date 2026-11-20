package rabbitmq

import (
	"context"
	"errors"
)

// DeduplicationStore tracks completed messages by queue and message ID.
type DeduplicationStore interface {
	IsProcessed(ctx context.Context, queue, messageID string) (bool, error)
	MarkProcessed(ctx context.Context, queue, messageID string) error
}

type DeduplicatingHandler struct {
	queue string
	inner Handler
	store DeduplicationStore
}

func NewDeduplicatingHandler(queue string, inner Handler, store DeduplicationStore) (*DeduplicatingHandler, error) {
	if queue == "" || inner == nil || store == nil {
		return nil, errors.New("rabbitmq: deduplication needs a queue, handler and store")
	}
	return &DeduplicatingHandler{queue: queue, inner: inner, store: store}, nil
}

func (h *DeduplicatingHandler) Handle(ctx context.Context, delivery Delivery) error {
	if delivery.MessageID == "" {
		return NewPermanentError(errors.New("rabbitmq: message_id is required for deduplication"))
	}
	// An alternative is to acquire a lease before processing to serialize concurrent duplicates.
	processed, err := h.store.IsProcessed(ctx, h.queue, delivery.MessageID)
	if err != nil || processed {
		return err
	}
	if err := h.inner.Handle(ctx, delivery); err != nil {
		return err
	}
	return h.store.MarkProcessed(ctx, h.queue, delivery.MessageID)
}
