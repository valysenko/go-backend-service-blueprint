package kafkaexample_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"go-backend-service-blueprint/internal/app/kafkaexample"
	"go-backend-service-blueprint/internal/app/kafkaexample/mocks"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestUserUseCasePublishesEventAndPropagatesError(t *testing.T) {
	ctx := context.Background()
	publisher := mocks.NewMockEventPublisher(t)
	failure := errors.New("publish failed")
	publisher.EXPECT().PublishEvent(ctx, []byte("user-key"), mock.MatchedBy(func(event kafkaexample.Event) bool {
		return event.ID != uuid.Nil && event.SomeField == "test@example.com"
	})).Return(failure).Once()
	uc := kafkaexample.NewUserUseCase(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher)
	err := uc.HandleUser(ctx, kafkaexample.UserCommand{UUID: uuid.New(), Email: "test@example.com", Key: []byte("user-key"), MessageID: "source-id"})
	require.ErrorIs(t, err, failure)
}

func TestUserUseCaseProducesSameEventIDOnRetry(t *testing.T) {
	ctx := context.Background()
	publisher := mocks.NewMockEventPublisher(t)
	var ids []uuid.UUID
	publisher.EXPECT().PublishEvent(ctx, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ []byte, event kafkaexample.Event) { ids = append(ids, event.ID) }).
		Return(nil).Twice()
	uc := kafkaexample.NewUserUseCase(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher)
	cmd := kafkaexample.UserCommand{UUID: uuid.New(), Email: "test@example.com", MessageID: "same-input"}
	require.NoError(t, uc.HandleUser(ctx, cmd))
	require.NoError(t, uc.HandleUser(ctx, cmd))
	require.Len(t, ids, 2)
	require.Equal(t, ids[0], ids[1])
}

func TestKafkaUseCasesDoNotLogPayload(t *testing.T) {
	ctx := context.Background()
	publisher := mocks.NewMockEventPublisher(t)
	publisher.EXPECT().PublishEvent(ctx, mock.Anything, mock.Anything).Return(nil).Once()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	user := kafkaexample.NewUserUseCase(logger, publisher)
	require.NoError(t, user.HandleUser(ctx, kafkaexample.UserCommand{UUID: uuid.New(), Email: "private@example.com", MessageID: "id"}))
	event := kafkaexample.NewEventUseCase(logger)
	require.NoError(t, event.HandleEvent(ctx, kafkaexample.EventCommand{EventID: uuid.New(), SomeField: "private@example.com"}))
	require.NotContains(t, logs.String(), "private@example.com")
}
