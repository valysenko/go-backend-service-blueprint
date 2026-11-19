package kafka

import (
	"context"
	"encoding/json"
	"go-backend-service-blueprint/pkg/kafka/writer"

	kafkago "github.com/segmentio/kafka-go"
)

type Publisher struct{ Writer writer.Writer }

func (p *Publisher) Publish(ctx context.Context, key []byte, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return p.Writer.WriteMessages(ctx, kafkago.Message{Key: key, Value: b})
}
