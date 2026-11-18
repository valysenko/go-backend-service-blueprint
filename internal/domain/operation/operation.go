package operation

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Operation is a unit of work planned for later execution by the cron binary.
type Operation struct {
	UUID          uuid.UUID
	OperationType OperationType
	EntityID      uuid.UUID
	Status        Status
	Payload       json.RawMessage
	Result        json.RawMessage
	PlannedAt     time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func New(operationType OperationType, entityID uuid.UUID, payload json.RawMessage, plannedAt, now time.Time) *Operation {
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}

	return &Operation{
		UUID:          uuid.New(),
		OperationType: operationType,
		EntityID:      entityID,
		Status:        StatusPlanned,
		Payload:       payload,
		PlannedAt:     plannedAt,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func (o *Operation) Complete(details json.RawMessage, now time.Time) {
	o.Status = StatusCompleted
	o.Result = marshalResult(map[string]any{
		"completed_at": now.Format(time.RFC3339),
		"details":      rawOrEmpty(details),
	})
	o.UpdatedAt = now
}

func (o *Operation) Fail(reason string, now time.Time) {
	o.Status = StatusFailed
	o.Result = marshalResult(map[string]any{
		"failed_at": now.Format(time.RFC3339),
		"reason":    reason,
	})
	o.UpdatedAt = now
}

func (o *Operation) Cancel(now time.Time) {
	o.Status = StatusCancelled
	o.UpdatedAt = now
}

func rawOrEmpty(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}

	return raw
}

func marshalResult(body map[string]any) json.RawMessage {
	encoded, err := json.Marshal(body)
	if err != nil {
		return json.RawMessage(`{"reason":"marshal_failed"}`)
	}

	return encoded
}
