package handler_test

import (
	"context"
	"errors"
	"testing"

	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/kafka/reader/handler"
	"go-backend-service-blueprint/pkg/kafka/reader/handler/mocks"

	kafkaPkg "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const handlerTestTopic = "test-topic"

func newHandlerTestMessage() *reader.Message {
	return reader.NewMessage(kafkaPkg.Message{Topic: handlerTestTopic})
}

func TestHandle(t *testing.T) {
	ctx := context.Background()

	t.Run("successful handle", func(t *testing.T) {
		processor := mocks.NewMockMessageProcessor(t)
		metricsRecorder := mocks.NewMockMetricsRecorder(t)
		h := handler.NewSingleMessageHandler(processor, metricsRecorder)
		msg := newHandlerTestMessage()

		processor.EXPECT().Process(ctx, msg).Return(nil).Once()
		metricsRecorder.EXPECT().ObserveEventProcessingDuration(handlerTestTopic, mock.AnythingOfType("float64")).Once()
		metricsRecorder.EXPECT().IncrementEventsTotal(handlerTestTopic, "success").Once()

		err := h.Handle(ctx, msg)

		assert.NoError(t, err)
	})

	t.Run("processor error", func(t *testing.T) {
		processor := mocks.NewMockMessageProcessor(t)
		metricsRecorder := mocks.NewMockMetricsRecorder(t)
		h := handler.NewSingleMessageHandler(processor, metricsRecorder)
		msg := newHandlerTestMessage()
		processErr := errors.New("processing failed")

		processor.EXPECT().Process(ctx, msg).Return(processErr).Once()
		metricsRecorder.EXPECT().ObserveEventProcessingDuration(handlerTestTopic, mock.AnythingOfType("float64")).Once()
		metricsRecorder.EXPECT().IncrementEventsTotal(handlerTestTopic, "error").Once()

		err := h.Handle(ctx, msg)

		assert.ErrorIs(t, err, processErr)
	})
}
