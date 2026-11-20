package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"go-backend-service-blueprint/pkg/rabbitmq"
)

type ExampleMessage struct {
	Text string `json:"text"`
}

type ExampleHandler struct {
	Logger *slog.Logger
	Queue  string
}

func (h *ExampleHandler) Handle(ctx context.Context, delivery rabbitmq.Delivery) error {
	var msg ExampleMessage
	if err := json.Unmarshal(delivery.Body, &msg); err != nil {
		return rabbitmq.NewPermanentError(fmt.Errorf("decode message: %w", err))
	}
	if msg.Text == "" {
		return rabbitmq.NewPermanentError(fmt.Errorf("text is required"))
	}
	h.Logger.InfoContext(ctx, "received rabbitmq message", "queue", h.Queue, "text", msg.Text, "attempt", delivery.Attempt)
	return nil
}
