package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go-backend-service-blueprint/pkg/rabbitmq"
)

type ExamplePublisher struct {
	Publisher *rabbitmq.Publisher
}

func (p *ExamplePublisher) Publish(ctx context.Context, id string, message any, delay time.Duration) error {
	if delay < 0 {
		return errors.New("rabbitmq: delay must not be negative")
	}
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}
	msg := rabbitmq.Message{ID: id, Body: body}
	if delay > 0 {
		return p.Publisher.PublishWithDelay(ctx, msg, delay)
	}
	return p.Publisher.Publish(ctx, msg)
}
