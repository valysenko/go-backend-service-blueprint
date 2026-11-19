package handler_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/kafka/reader/handler"
	handlerMocks "go-backend-service-blueprint/pkg/kafka/reader/handler/mocks"
	"go-backend-service-blueprint/pkg/kafka/reader/mocks"
	writerMocks "go-backend-service-blueprint/pkg/kafka/writer/mocks"

	kafkaPkg "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const dlTopic = "test-topic-dl"

func newFallbackTestMessage() *reader.Message {
	return reader.NewMessage(kafkaPkg.Message{
		Topic: handlerTestTopic,
		Key:   []byte("pk"),
		Value: []byte("payload"),
	})
}

func matchDLWrite(src *reader.Message) any {
	return mock.MatchedBy(func(msgs []kafkaPkg.Message) bool {
		return len(msgs) == 1 &&
			bytes.Equal(msgs[0].Key, src.Key()) &&
			bytes.Equal(msgs[0].Value, src.Body())
	})
}

func TestDLFallbackHandle(t *testing.T) {
	ctx := context.Background()
	logger := newRetryTestLogger()

	t.Run("count > 0: writes to DL and increments counter", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlIncrementStorage(t)
		w := writerMocks.NewMockWriter(t)
		h := handler.NewDLFallbackMessageHandler(inner, storage, w, logger, dlTopic)
		msg := newFallbackTestMessage()

		storage.EXPECT().GetDlCounter(ctx, msg).Return(2, nil).Once()
		w.EXPECT().WriteMessages(ctx, matchDLWrite(msg)).Return(nil).Once()
		storage.EXPECT().IncrementCounter(ctx, msg).Return(nil).Once()

		err := h.Handle(ctx, msg)

		inner.AssertNotCalled(t, "Handle")
		assert.NoError(t, err)
	})

	t.Run("count == 0 + inner success: no DL write", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlIncrementStorage(t)
		w := writerMocks.NewMockWriter(t)
		h := handler.NewDLFallbackMessageHandler(inner, storage, w, logger, dlTopic)
		msg := newFallbackTestMessage()

		storage.EXPECT().GetDlCounter(ctx, msg).Return(0, nil).Once()
		inner.EXPECT().Handle(ctx, msg).Return(nil).Once()

		err := h.Handle(ctx, msg)

		storage.AssertNotCalled(t, "IncrementCounter")
		assert.NoError(t, err)
	})

	t.Run("count == 0 + inner DiscardError: skip DL, return nil", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlIncrementStorage(t)
		w := writerMocks.NewMockWriter(t)
		h := handler.NewDLFallbackMessageHandler(inner, storage, w, logger, dlTopic)
		msg := newFallbackTestMessage()
		discardErr := reader.NewDiscardError(errors.New("ignore"), "skip")

		storage.EXPECT().GetDlCounter(ctx, msg).Return(0, nil).Once()
		inner.EXPECT().Handle(ctx, msg).Return(discardErr).Once()

		err := h.Handle(ctx, msg)

		storage.AssertNotCalled(t, "IncrementCounter")
		assert.NoError(t, err)
	})

	t.Run("count == 0 + inner ReaderFailure: skip DL, propagate error", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlIncrementStorage(t)
		w := writerMocks.NewMockWriter(t)
		h := handler.NewDLFallbackMessageHandler(inner, storage, w, logger, dlTopic)
		msg := newFallbackTestMessage()
		rfErr := reader.NewReaderFailure(errors.New("processor db down"), "processor")

		storage.EXPECT().GetDlCounter(ctx, msg).Return(0, nil).Once()
		inner.EXPECT().Handle(ctx, msg).Return(rfErr).Once()

		err := h.Handle(ctx, msg)

		w.AssertNotCalled(t, "WriteMessages")
		storage.AssertNotCalled(t, "IncrementCounter")

		var target *reader.ReaderFailure
		assert.ErrorAs(t, err, &target)
	})

	t.Run("count == 0 + inner error: writes to DL and increments", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlIncrementStorage(t)
		w := writerMocks.NewMockWriter(t)
		h := handler.NewDLFallbackMessageHandler(inner, storage, w, logger, dlTopic)
		msg := newFallbackTestMessage()

		storage.EXPECT().GetDlCounter(ctx, msg).Return(0, nil).Once()
		inner.EXPECT().Handle(ctx, msg).Return(errors.New("processing failed")).Once()
		w.EXPECT().WriteMessages(ctx, matchDLWrite(msg)).Return(nil).Once()
		storage.EXPECT().IncrementCounter(ctx, msg).Return(nil).Once()

		err := h.Handle(ctx, msg)

		assert.NoError(t, err)
	})

	t.Run("GetDlCounter error: returns wrapped error, inner not called", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlIncrementStorage(t)
		w := writerMocks.NewMockWriter(t)
		h := handler.NewDLFallbackMessageHandler(inner, storage, w, logger, dlTopic)
		msg := newFallbackTestMessage()
		expectedErr := errors.New("redis down")

		storage.EXPECT().GetDlCounter(ctx, msg).Return(0, expectedErr).Once()

		err := h.Handle(ctx, msg)

		inner.AssertNotCalled(t, "Handle")
		storage.AssertNotCalled(t, "IncrementCounter")
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("WriteMessages error: returns wrapped error", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlIncrementStorage(t)
		w := writerMocks.NewMockWriter(t)
		h := handler.NewDLFallbackMessageHandler(inner, storage, w, logger, dlTopic)
		msg := newFallbackTestMessage()
		pubErr := errors.New("kafka down")

		storage.EXPECT().GetDlCounter(ctx, msg).Return(2, nil).Once()
		storage.EXPECT().IncrementCounter(ctx, msg).Return(nil).Once()
		storage.EXPECT().DecrementCounter(ctx, msg).Return(nil).Once()
		w.EXPECT().WriteMessages(ctx, mock.Anything).Return(pubErr).Once()

		err := h.Handle(ctx, msg)

		inner.AssertNotCalled(t, "Handle")
		assert.ErrorIs(t, err, pubErr)
	})

	t.Run("IncrementCounter error prevents publishing", func(t *testing.T) {
		inner := mocks.NewMockMessageHandler(t)
		storage := handlerMocks.NewMockDlIncrementStorage(t)
		w := writerMocks.NewMockWriter(t)
		h := handler.NewDLFallbackMessageHandler(inner, storage, w, logger, dlTopic)
		msg := newFallbackTestMessage()
		incrErr := errors.New("redis down")

		storage.EXPECT().GetDlCounter(ctx, msg).Return(2, nil).Once()
		storage.EXPECT().IncrementCounter(ctx, msg).Return(incrErr).Once()

		err := h.Handle(ctx, msg)

		inner.AssertNotCalled(t, "Handle")
		w.AssertNotCalled(t, "WriteMessages")
		assert.ErrorIs(t, err, incrErr)
	})
}
