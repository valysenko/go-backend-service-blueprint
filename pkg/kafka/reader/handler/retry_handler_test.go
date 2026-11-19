package handler_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/kafka/reader/handler"
	"go-backend-service-blueprint/pkg/kafka/reader/mocks"

	"github.com/stretchr/testify/assert"
)

func newRetryTestLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func newFastRetryConfig(totalAttempts int) handler.RetryConfig {
	return handler.MustRetryConfig(totalAttempts, 1, 1)
}

func TestRetryHandle(t *testing.T) {
	ctx := context.Background()
	logger := newRetryTestLogger()

	t.Run("success on first attempt", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewRetryMessageHandler(inner, newFastRetryConfig(3), logger)
		msg := newHandlerTestMessage()

		inner.EXPECT().Handle(ctx, msg).Return(nil).Once()

		err := h.Handle(ctx, msg)

		assert.NoError(t, err)
	})

	t.Run("succeeds after transient failures", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewRetryMessageHandler(inner, newFastRetryConfig(3), logger)
		msg := newHandlerTestMessage()

		// 3 attempts - 3rd is successful
		inner.EXPECT().Handle(ctx, msg).Return(errors.New("transient")).Twice()
		inner.EXPECT().Handle(ctx, msg).Return(nil).Once()

		err := h.Handle(ctx, msg)

		assert.NoError(t, err)
	})

	t.Run("returns last error after exhausting all attempts", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewRetryMessageHandler(inner, newFastRetryConfig(3), logger)
		msg := newHandlerTestMessage()
		lastErr := errors.New("still failing")

		inner.EXPECT().Handle(ctx, msg).Return(errors.New("first")).Once()
		inner.EXPECT().Handle(ctx, msg).Return(errors.New("second")).Once()
		inner.EXPECT().Handle(ctx, msg).Return(lastErr).Once()

		err := h.Handle(ctx, msg)

		assert.ErrorIs(t, err, lastErr)
	})

	t.Run("UnrecoverableError without retrying", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewRetryMessageHandler(inner, newFastRetryConfig(3), logger)
		msg := newHandlerTestMessage()
		unrecErr := reader.NewUnrecoverableError(errors.New("bad data"), "parse")

		inner.EXPECT().Handle(ctx, msg).Return(unrecErr).Once()

		err := h.Handle(ctx, msg)

		var target *reader.UnrecoverableError
		assert.ErrorAs(t, err, &target)
	})

	t.Run("DiscardError without retrying", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewRetryMessageHandler(inner, newFastRetryConfig(3), logger)
		msg := newHandlerTestMessage()
		discardErr := reader.NewDiscardError(errors.New("ignore"), "skip")

		inner.EXPECT().Handle(ctx, msg).Return(discardErr).Once()

		err := h.Handle(ctx, msg)

		var target *reader.DiscardError
		assert.ErrorAs(t, err, &target)
	})

	t.Run("ReaderFailure without retrying", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewRetryMessageHandler(inner, newFastRetryConfig(3), logger)
		msg := newHandlerTestMessage()
		rfErr := reader.NewReaderFailure(errors.New("downstream down"), "processor X")

		// .Once() — confirms no retries; if the handler retried, the second call
		// would trip testify's strict mode and fail the test.
		inner.EXPECT().Handle(ctx, msg).Return(rfErr).Once()

		err := h.Handle(ctx, msg)

		var target *reader.ReaderFailure
		assert.ErrorAs(t, err, &target)
	})

	t.Run("aborts backoff and returns ctx error when ctx is cancelled mid-retry", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		// long backoff so the ctx cancel happens earlier that <-time.After
		cfg := handler.MustRetryConfig(3, 500, 1)
		h := handler.NewRetryMessageHandler(inner, cfg, logger)
		msg := newHandlerTestMessage()

		cancelCtx, cancel := context.WithCancel(ctx)
		inner.EXPECT().Handle(cancelCtx, msg).RunAndReturn(func(_ context.Context, _ *reader.Message) error {
			// cancel before return so next attempt won't happen
			cancel()
			return errors.New("transient")
		}).Once()

		err := h.Handle(cancelCtx, msg)

		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestNewRetryConfig(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg, err := handler.NewRetryConfig(3, 100, 2)
		assert.NoError(t, err)
		assert.NotZero(t, cfg)
	})

	t.Run("rejects totalAttempts < 1", func(t *testing.T) {
		_, err := handler.NewRetryConfig(0, 100, 2)
		assert.Error(t, err)
	})

	t.Run("rejects backoffBaseMs < 1", func(t *testing.T) {
		_, err := handler.NewRetryConfig(3, 0, 2)
		assert.Error(t, err)
	})

	t.Run("rejects backoffCoefficient < 1", func(t *testing.T) {
		_, err := handler.NewRetryConfig(3, 100, 0)
		assert.Error(t, err)
	})
}
