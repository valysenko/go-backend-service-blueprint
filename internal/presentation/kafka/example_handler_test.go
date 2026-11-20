package kafka_test

import (
	"context"
	"errors"
	"testing"

	"go-backend-service-blueprint/internal/app/kafkaexample"
	example "go-backend-service-blueprint/internal/presentation/kafka"
	"go-backend-service-blueprint/internal/presentation/kafka/mocks"
	"go-backend-service-blueprint/pkg/kafka/reader"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func TestUserHandlerPassesCommandAndReturnsError(t *testing.T) {
	ctx := context.Background()
	useCase := mocks.NewMockUserUseCase(t)
	id := uuid.MustParse("4c979df6-0b4f-4d38-a06d-f54e2325890e")
	failure := errors.New("use case failed")
	msg := reader.NewMessage(kafkago.Message{Key: []byte("user-key"), Value: []byte(`{"uuid":"4c979df6-0b4f-4d38-a06d-f54e2325890e","email":"test@example.com"}`)})
	useCase.EXPECT().HandleUser(ctx, kafkaexample.UserCommand{UUID: id, Email: "test@example.com", Key: []byte("user-key"), MessageID: msg.StableID()}).Return(failure).Once()
	h := example.UserHandler{UseCase: useCase}
	require.ErrorIs(t, h.Process(ctx, msg), failure)
}

func TestEventHandlerPassesCommandAndReturnsError(t *testing.T) {
	ctx := context.Background()
	useCase := mocks.NewMockEventUseCase(t)
	id := uuid.MustParse("4c979df6-0b4f-4d38-a06d-f54e2325890e")
	failure := errors.New("use case failed")
	useCase.EXPECT().HandleEvent(ctx, kafkaexample.EventCommand{EventID: id, SomeField: "example"}).Return(failure).Once()
	h := example.EventHandler{UseCase: useCase}
	msg := reader.NewMessage(kafkago.Message{Value: []byte(`{"eventId":"4c979df6-0b4f-4d38-a06d-f54e2325890e","someField":"example"}`)})
	require.ErrorIs(t, h.Process(ctx, msg), failure)
}
