package rabbitmqexample_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"go-backend-service-blueprint/internal/app/rabbitmqexample"
	"go-backend-service-blueprint/internal/app/rabbitmqexample/mocks"

	"github.com/stretchr/testify/require"
)

func TestUseCasePublishesProcessedMessage(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("publisher unavailable")
	publisher := mocks.NewMockPublisher(t)
	publisher.EXPECT().PublishProcessed(ctx, rabbitmqexample.ProcessedMessage{
		SourceQueue: "example.direct",
		Text:        "hello",
	}).Return(failure).Once()
	uc := rabbitmqexample.NewUseCase(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher)
	err := uc.Execute(ctx, rabbitmqexample.Command{Queue: "example.direct", Text: "hello", Attempt: 1})
	require.ErrorIs(t, err, failure)
}

func TestUseCaseReturnsNilAfterPublishing(t *testing.T) {
	ctx := context.Background()
	publisher := mocks.NewMockPublisher(t)
	publisher.EXPECT().PublishProcessed(ctx, rabbitmqexample.ProcessedMessage{
		SourceQueue: "example.delayed",
		Text:        "hello",
	}).Return(nil).Once()
	uc := rabbitmqexample.NewUseCase(slog.New(slog.NewTextHandler(io.Discard, nil)), publisher)
	require.NoError(t, uc.Execute(ctx, rabbitmqexample.Command{Queue: "example.delayed", Text: "hello", Attempt: 1}))
}

func TestUseCaseDoesNotLogMessageText(t *testing.T) {
	ctx := context.Background()
	publisher := mocks.NewMockPublisher(t)
	publisher.EXPECT().PublishProcessed(ctx, rabbitmqexample.ProcessedMessage{
		SourceQueue: "example.direct", Text: "private-payload",
	}).Return(nil).Once()
	var logs bytes.Buffer
	uc := rabbitmqexample.NewUseCase(slog.New(slog.NewJSONHandler(&logs, nil)), publisher)
	require.NoError(t, uc.Execute(ctx, rabbitmqexample.Command{Queue: "example.direct", Text: "private-payload"}))
	require.NotContains(t, logs.String(), "private-payload")
}
