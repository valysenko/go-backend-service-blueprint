package handler_test

import (
	"context"
	"errors"
	"testing"

	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/kafka/reader/handler"
	handlerMocks "go-backend-service-blueprint/pkg/kafka/reader/handler/mocks"
	"go-backend-service-blueprint/pkg/kafka/reader/mocks"

	"github.com/stretchr/testify/assert"
)

func TestDLTopicHandle(t *testing.T) {
	ctx := context.Background()
	logger := newRetryTestLogger()

	t.Run("inner success first try: decrements and returns nil", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlDecrementStorage(t)
		h := handler.NewDLTopicMessageHandler(inner, storage, logger, 3)
		msg := newFallbackTestMessage()

		inner.EXPECT().Handle(ctx, msg).Return(nil).Once()
		storage.EXPECT().DecrementCounter(ctx, msg).Return(nil).Once()

		err := h.Handle(ctx, msg)

		assert.NoError(t, err)
	})

	t.Run("inner DiscardError: decrements and returns nil", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlDecrementStorage(t)
		h := handler.NewDLTopicMessageHandler(inner, storage, logger, 3)
		msg := newFallbackTestMessage()
		discardErr := reader.NewDiscardError(errors.New("ignore"), "skip")

		inner.EXPECT().Handle(ctx, msg).Return(discardErr).Once()
		storage.EXPECT().DecrementCounter(ctx, msg).Return(nil).Once()

		err := h.Handle(ctx, msg)

		assert.NoError(t, err)
	})

	t.Run("inner ReaderFailure: skip retries and decrement, propagate error", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlDecrementStorage(t)
		h := handler.NewDLTopicMessageHandler(inner, storage, logger, 3)
		msg := newFallbackTestMessage()
		rfErr := reader.NewReaderFailure(errors.New("processor db down"), "processor")

		// .Once() — no retries
		inner.EXPECT().Handle(ctx, msg).Return(rfErr).Once()

		err := h.Handle(ctx, msg)
		storage.AssertNotCalled(t, "DecrementCounter")

		var target *reader.ReaderFailure
		assert.ErrorAs(t, err, &target)
	})

	t.Run("inner DiscardError + DecrementCounter fails: returns wrapped error", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlDecrementStorage(t)
		h := handler.NewDLTopicMessageHandler(inner, storage, logger, 3)
		msg := newFallbackTestMessage()
		discardErr := reader.NewDiscardError(errors.New("ignore"), "skip")
		decErr := errors.New("redis down")

		inner.EXPECT().Handle(ctx, msg).Return(discardErr).Once()
		storage.EXPECT().DecrementCounter(ctx, msg).Return(decErr).Once()

		err := h.Handle(ctx, msg)

		assert.ErrorIs(t, err, decErr)
	})

	t.Run("maxAttempts exhausted: decrements and returns nil", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlDecrementStorage(t)
		h := handler.NewDLTopicMessageHandler(inner, storage, logger, 1)
		msg := newFallbackTestMessage()

		inner.EXPECT().Handle(ctx, msg).Return(errors.New("processing failed")).Once()
		storage.EXPECT().DecrementCounter(ctx, msg).Return(nil).Once()

		err := h.Handle(ctx, msg)

		assert.NoError(t, err)
	})

	t.Run("maxAttempts exhausted + DecrementCounter fails: returns wrapped error", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlDecrementStorage(t)
		h := handler.NewDLTopicMessageHandler(inner, storage, logger, 1)
		msg := newFallbackTestMessage()
		decErr := errors.New("redis down")

		inner.EXPECT().Handle(ctx, msg).Return(errors.New("processing failed")).Once()
		storage.EXPECT().DecrementCounter(ctx, msg).Return(decErr).Once()

		err := h.Handle(ctx, msg)

		assert.ErrorIs(t, err, decErr)
	})

	t.Run("inner success + DecrementCounter error: returns wrapped error", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlDecrementStorage(t)
		h := handler.NewDLTopicMessageHandler(inner, storage, logger, 3)
		msg := newFallbackTestMessage()
		decErr := errors.New("redis down")

		inner.EXPECT().Handle(ctx, msg).Return(nil).Once()
		storage.EXPECT().DecrementCounter(ctx, msg).Return(decErr).Once()

		err := h.Handle(ctx, msg)

		assert.ErrorIs(t, err, decErr)
	})

	t.Run("ctx cancelled mid-retry: returns ctx error", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlDecrementStorage(t)
		// maxAttempts=2 so the loop reaches the backoff select after first failure
		h := handler.NewDLTopicMessageHandler(inner, storage, logger, 2)
		msg := newFallbackTestMessage()

		cancelCtx, cancel := context.WithCancel(ctx)
		inner.EXPECT().Handle(cancelCtx, msg).RunAndReturn(func(_ context.Context, _ *reader.Message) error {
			// cancel before return so next attempt won't happen
			cancel()
			return errors.New("transient")
		}).Once()

		err := h.Handle(cancelCtx, msg)

		assert.ErrorIs(t, err, context.Canceled)
		storage.AssertNotCalled(t, "DecrementCounter")
	})
}
