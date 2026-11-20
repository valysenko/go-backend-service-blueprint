package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"go-backend-service-blueprint/internal/app/rabbitmqexample"
	"go-backend-service-blueprint/pkg/rabbitmq"
)

type ExampleMessage struct {
	Text string `json:"text"`
}

type UseCase interface {
	Execute(context.Context, rabbitmqexample.Command) error
}

type ExampleHandler struct {
	UseCase UseCase
	Queue   string
}

func (h *ExampleHandler) Handle(ctx context.Context, delivery rabbitmq.Delivery) error {
	var msg ExampleMessage
	if err := json.Unmarshal(delivery.Body, &msg); err != nil {
		return rabbitmq.NewPermanentError(fmt.Errorf("decode message: %w", err))
	}
	if msg.Text == "" {
		return rabbitmq.NewPermanentError(fmt.Errorf("text is required"))
	}
	return h.UseCase.Execute(ctx, rabbitmqexample.Command{
		MessageID: delivery.MessageID,
		Queue:     h.Queue,
		Text:      msg.Text,
		Attempt:   delivery.Attempt,
	})
}
