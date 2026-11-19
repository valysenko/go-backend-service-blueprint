package handler

import (
	"context"
	"go-backend-service-blueprint/pkg/kafka/reader"
	"time"
)

type MessageProcessor interface {
	Process(ctx context.Context, msg *reader.Message) error
}

type MetricsRecorder interface {
	IncrementEventsTotal(topic, status string)
	ObserveEventProcessingDuration(topic string, durationSeconds float64)
}

// SingleMessageHandler is the leaf of both BaseFlow and DLFlow chains.
// executes domain MessageProcessor and records metrics:
// processing duration, success/error count
type SingleMessageHandler struct {
	processor MessageProcessor
	metrics   MetricsRecorder
}

func NewSingleMessageHandler(processor MessageProcessor, metrics MetricsRecorder) *SingleMessageHandler {
	return &SingleMessageHandler{
		processor: processor,
		metrics:   metrics,
	}
}

func (h *SingleMessageHandler) Handle(ctx context.Context, msg *reader.Message) error {
	start := time.Now()

	err := h.processor.Process(ctx, msg)
	duration := time.Since(start).Seconds()

	h.metrics.ObserveEventProcessingDuration(msg.Topic(), duration)

	if err != nil {
		h.metrics.IncrementEventsTotal(msg.Topic(), "error")
		return err
	}

	h.metrics.IncrementEventsTotal(msg.Topic(), "success")
	return nil
}
