package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"go-backend-service-blueprint/pkg/kafka/reader"

	"github.com/google/uuid"
)

type Publisher interface {
	Publish(context.Context, []byte, any) error
}

type UserMessage struct {
	UUID  uuid.UUID `json:"uuid"`
	Email string    `json:"email"`
}

type EventMessage struct {
	EventID   uuid.UUID `json:"eventId"`
	SomeField string    `json:"someField"`
}

type UserHandler struct {
	Logger    *slog.Logger
	Publisher Publisher
}

// Process
func (h *UserHandler) Process(ctx context.Context, msg *reader.Message) error {
	var user UserMessage
	if err := json.Unmarshal(msg.Body(), &user); err != nil {
		return fmt.Errorf("decode user: %w", err)
	}
	h.Logger.InfoContext(ctx, "received user", "uuid", user.UUID, "email", user.Email)
	return h.Publisher.Publish(ctx, msg.Key(), EventMessage{EventID: uuid.New(), SomeField: user.Email})
}

type EventHandler struct{ Logger *slog.Logger }

func (h *EventHandler) Process(ctx context.Context, msg *reader.Message) error {
	var event EventMessage
	if err := json.Unmarshal(msg.Body(), &event); err != nil {
		return fmt.Errorf("decode event: %w", err)
	}
	h.Logger.InfoContext(ctx, "received event", "eventId", event.EventID, "someField", event.SomeField)
	return nil
}
