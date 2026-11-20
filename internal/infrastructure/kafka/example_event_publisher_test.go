package kafka_test

import (
	"context"
	"encoding/json"
	"testing"

	"go-backend-service-blueprint/internal/app/kafkaexample"
	kafka "go-backend-service-blueprint/internal/infrastructure/kafka"
	"go-backend-service-blueprint/pkg/kafka/writer/mocks"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPublisherWritesEvent(t *testing.T) {
	ctx := context.Background()
	id := uuid.New()
	writer := mocks.NewMockWriter(t)
	writer.EXPECT().WriteMessages(ctx, mock.MatchedBy(func(messages []kafkago.Message) bool {
		if len(messages) != 1 {
			return false
		}
		msg := messages[0]
		var payload struct {
			EventID   uuid.UUID `json:"eventId"`
			SomeField string    `json:"someField"`
		}
		return string(msg.Key) == "user-key" && json.Unmarshal(msg.Value, &payload) == nil && payload.EventID == id && payload.SomeField == "hello"
	})).Return(nil).Once()
	p := kafka.Publisher{Writer: writer}
	require.NoError(t, p.PublishEvent(ctx, []byte("user-key"), kafkaexample.Event{ID: id, SomeField: "hello"}))
}
