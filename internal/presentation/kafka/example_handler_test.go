package kafka_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	example "go-backend-service-blueprint/internal/presentation/kafka"
	"go-backend-service-blueprint/pkg/kafka/reader"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type capturePublisher struct {
	key   []byte
	value any
}

func (p *capturePublisher) Publish(_ context.Context, key []byte, value any) error {
	p.key, p.value = key, value
	return nil
}

func TestUserHandlerPublishesEvent(t *testing.T) {
	p := &capturePublisher{}
	h := example.UserHandler{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Publisher: p}
	msg := reader.NewMessage(kafkago.Message{Key: []byte("user-key"), Value: []byte(`{"uuid":"4c979df6-0b4f-4d38-a06d-f54e2325890e","email":"test@example.com"}`)})
	require.NoError(t, h.Process(context.Background(), msg))
	require.Equal(t, []byte("user-key"), p.key)
	event, ok := p.value.(example.EventMessage)
	require.True(t, ok)
	require.NotEqual(t, uuid.Nil, event.EventID)
	require.Equal(t, "test@example.com", event.SomeField)
}
