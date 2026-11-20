package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"go-backend-service-blueprint/internal/app/kafkaexample"
	"go-backend-service-blueprint/pkg/kafka/reader"

	"github.com/google/uuid"
)

type UserMessage struct {
	UUID  uuid.UUID `json:"uuid"`
	Email string    `json:"email"`
}

type EventMessage struct {
	EventID   uuid.UUID `json:"eventId"`
	SomeField string    `json:"someField"`
}

type UserUseCase interface {
	HandleUser(context.Context, kafkaexample.UserCommand) error
}

type EventUseCase interface {
	HandleEvent(context.Context, kafkaexample.EventCommand) error
}

type UserHandler struct {
	UseCase UserUseCase
}

func (h *UserHandler) Process(ctx context.Context, msg *reader.Message) error {
	var user UserMessage
	if err := json.Unmarshal(msg.Body(), &user); err != nil {
		return fmt.Errorf("decode user: %w", err)
	}
	if err := h.UseCase.HandleUser(ctx, kafkaexample.UserCommand{
		UUID: user.UUID, Email: user.Email, Key: msg.Key(), MessageID: msg.StableID(),
	}); err != nil {
		return fmt.Errorf("handle user: %w", err)
	}
	return nil
}

type EventHandler struct{ UseCase EventUseCase }

func (h *EventHandler) Process(ctx context.Context, msg *reader.Message) error {
	var event EventMessage
	if err := json.Unmarshal(msg.Body(), &event); err != nil {
		return fmt.Errorf("decode event: %w", err)
	}
	return h.UseCase.HandleEvent(ctx, kafkaexample.EventCommand{EventID: event.EventID, SomeField: event.SomeField})
}
