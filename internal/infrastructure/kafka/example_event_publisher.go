package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"go-backend-service-blueprint/internal/app/kafkaexample"
	"go-backend-service-blueprint/pkg/kafka/writer"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

type Publisher struct{ Writer writer.Writer }

type eventMessage struct {
	EventID   uuid.UUID `json:"eventId"`
	SomeField string    `json:"someField"`
}

func (p *Publisher) PublishEvent(ctx context.Context, key []byte, event kafkaexample.Event) error {
	body, err := json.Marshal(eventMessage{EventID: event.ID, SomeField: event.SomeField})
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return p.Writer.WriteMessages(ctx, kafkago.Message{
		Key:     key,
		Value:   body,
		Headers: []kafkago.Header{{Key: "id", Value: []byte(event.ID.String())}},
	})
}
