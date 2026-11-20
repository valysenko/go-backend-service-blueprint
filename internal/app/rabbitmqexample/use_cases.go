package rabbitmqexample

import (
	"context"
	"fmt"
	"log/slog"
)

type Command struct {
	MessageID string
	Queue     string
	Text      string
	Attempt   int
}

type ProcessedMessage struct {
	MessageID   string
	SourceQueue string
	Text        string
}

type Publisher interface {
	PublishProcessed(context.Context, ProcessedMessage) error
}

type UseCase struct {
	logger    *slog.Logger
	publisher Publisher
}

func NewUseCase(logger *slog.Logger, publisher Publisher) *UseCase {
	return &UseCase{logger: logger, publisher: publisher}
}

func (uc *UseCase) Execute(ctx context.Context, cmd Command) error {
	uc.logger.InfoContext(ctx, "processed rabbitmq message", "queue", cmd.Queue, "attempt", cmd.Attempt)
	message := ProcessedMessage{MessageID: cmd.MessageID, SourceQueue: cmd.Queue, Text: cmd.Text}
	if err := uc.publisher.PublishProcessed(ctx, message); err != nil {
		return fmt.Errorf("publish processed message: %w", err)
	}
	return nil
}
