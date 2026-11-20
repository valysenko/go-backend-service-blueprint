package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go-backend-service-blueprint/internal/app/rabbitmqexample"
	"go-backend-service-blueprint/pkg/rabbitmq"

	"github.com/google/uuid"
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

// PublishProcessed publishes a derived message to the configured output queue.
func (p *ExamplePublisher) PublishProcessed(ctx context.Context, msg rabbitmqexample.ProcessedMessage) error {
	payload := struct {
		SourceQueue string `json:"sourceQueue"`
		Text        string `json:"text"`
	}{SourceQueue: msg.SourceQueue, Text: msg.Text}
	// A retry of the same input must produce the same ID, so downstream
	// consumers can deduplicate even if the publish succeeded before a crash.
	id := uuid.NewSHA1(uuid.NameSpaceURL, []byte("rabbitmq:processed:"+msg.SourceQueue+"\x00"+msg.MessageID))
	return p.Publish(ctx, id.String(), payload, 0)
}
