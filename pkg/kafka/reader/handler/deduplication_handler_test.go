package handler_test

import (
	"context"
	"errors"
	"testing"

	"go-backend-service-blueprint/pkg/kafka/reader/handler"
	handlerMocks "go-backend-service-blueprint/pkg/kafka/reader/handler/mocks"
	"go-backend-service-blueprint/pkg/kafka/reader/mocks"

	"github.com/stretchr/testify/assert"
)

func TestDeduplicationHandle(t *testing.T) {
	ctx := context.Background()

	t.Run("not acked: inner runs and Ack is called", func(t *testing.T) {
		storage := handlerMocks.NewMockDeduplicationStorage(t)
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewDeduplicationMessageHandler(storage, inner)
		msg := newFallbackTestMessage()

		storage.EXPECT().IsAcked(ctx, msg).Return(false, nil).Once()
		inner.EXPECT().Handle(ctx, msg).Return(nil).Once()
		storage.EXPECT().Ack(ctx, msg).Return(nil).Once()

		err := h.Handle(ctx, msg)

		assert.NoError(t, err)
	})

	t.Run("already acked: skip inner and Ack, return nil", func(t *testing.T) {
		storage := handlerMocks.NewMockDeduplicationStorage(t)
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewDeduplicationMessageHandler(storage, inner)
		msg := newFallbackTestMessage()

		storage.EXPECT().IsAcked(ctx, msg).Return(true, nil).Once()

		err := h.Handle(ctx, msg)

		inner.AssertNotCalled(t, "Handle")
		storage.AssertNotCalled(t, "Ack")
		assert.NoError(t, err)
	})

	t.Run("IsAcked error: returns error, no inner, no Ack", func(t *testing.T) {
		storage := handlerMocks.NewMockDeduplicationStorage(t)
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewDeduplicationMessageHandler(storage, inner)
		msg := newFallbackTestMessage()
		expectedErr := errors.New("redis down")

		storage.EXPECT().IsAcked(ctx, msg).Return(false, expectedErr).Once()

		err := h.Handle(ctx, msg)

		inner.AssertNotCalled(t, "Handle")
		storage.AssertNotCalled(t, "Ack")
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("inner error: returns error, no Ack", func(t *testing.T) {
		storage := handlerMocks.NewMockDeduplicationStorage(t)
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewDeduplicationMessageHandler(storage, inner)
		msg := newFallbackTestMessage()
		innerErr := errors.New("processing failed")

		storage.EXPECT().IsAcked(ctx, msg).Return(false, nil).Once()
		inner.EXPECT().Handle(ctx, msg).Return(innerErr).Once()

		err := h.Handle(ctx, msg)

		storage.AssertNotCalled(t, "Ack")
		assert.ErrorIs(t, err, innerErr)
	})

	t.Run("Ack error after successful inner: returns Ack error", func(t *testing.T) {
		storage := handlerMocks.NewMockDeduplicationStorage(t)
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewDeduplicationMessageHandler(storage, inner)
		msg := newFallbackTestMessage()
		ackErr := errors.New("redis down")

		storage.EXPECT().IsAcked(ctx, msg).Return(false, nil).Once()
		inner.EXPECT().Handle(ctx, msg).Return(nil).Once()
		storage.EXPECT().Ack(ctx, msg).Return(ackErr).Once()

		err := h.Handle(ctx, msg)

		assert.ErrorIs(t, err, ackErr)
	})

	t.Run("ctx cancelled: returns ctx error, no IsAcked, no inner, no Ack", func(t *testing.T) {
		storage := handlerMocks.NewMockDeduplicationStorage(t)
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewDeduplicationMessageHandler(storage, inner)
		msg := newFallbackTestMessage()

		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()

		err := h.Handle(cancelledCtx, msg)

		storage.AssertNotCalled(t, "IsAcked")
		storage.AssertNotCalled(t, "Ack")
		inner.AssertNotCalled(t, "Handle")
		assert.ErrorIs(t, err, context.Canceled)
	})
}
