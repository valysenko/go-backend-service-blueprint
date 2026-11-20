package kafkaexample

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

type UserCommand struct {
	UUID      uuid.UUID
	Email     string
	Key       []byte
	MessageID string
}

type EventCommand struct {
	EventID   uuid.UUID
	SomeField string
}

type Event struct {
	ID        uuid.UUID
	SomeField string
}

type EventPublisher interface {
	PublishEvent(context.Context, []byte, Event) error
}

type UserUseCase struct {
	logger    *slog.Logger
	publisher EventPublisher
}

func NewUserUseCase(logger *slog.Logger, publisher EventPublisher) *UserUseCase {
	return &UserUseCase{logger: logger, publisher: publisher}
}

func (uc *UserUseCase) HandleUser(ctx context.Context, cmd UserCommand) error {
	if cmd.MessageID == "" {
		return errors.New("user message ID is required")
	}
	uc.logger.InfoContext(ctx, "processed user", "uuid", cmd.UUID)
	id := uuid.NewSHA1(uuid.NameSpaceURL, []byte("kafka:user-event:"+cmd.MessageID))
	if err := uc.publisher.PublishEvent(ctx, cmd.Key, Event{ID: id, SomeField: cmd.Email}); err != nil {
		return fmt.Errorf("publish user event: %w", err)
	}
	return nil
}

type EventUseCase struct {
	logger *slog.Logger
}

func NewEventUseCase(logger *slog.Logger) *EventUseCase {
	return &EventUseCase{logger: logger}
}

func (uc *EventUseCase) HandleEvent(ctx context.Context, cmd EventCommand) error {
	uc.logger.InfoContext(ctx, "processed event", "eventId", cmd.EventID)
	return nil
}
