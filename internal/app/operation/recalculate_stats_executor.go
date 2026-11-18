package operation

import (
	"context"
	"encoding/json"

	"go-backend-service-blueprint/internal/domain/operation"
)

type RecalculateStatsExecutor struct{}

var _ Executor = (*RecalculateStatsExecutor)(nil)

func NewRecalculateStatsExecutor() *RecalculateStatsExecutor {
	return &RecalculateStatsExecutor{}
}

type recalculateStatsPayload struct {
	Days int `json:"days"`
}

func (e *RecalculateStatsExecutor) Execute(ctx context.Context, op *operation.Operation) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, &operation.RecoverableError{Cause: err}
	}

	var payload recalculateStatsPayload
	if err := json.Unmarshal(op.Payload, &payload); err != nil {
		return nil, &operation.FailedError{Reason: operation.ReasonMalformedPayload}
	}
	if payload.Days <= 0 {
		return nil, &operation.FailedError{Reason: operation.ReasonMalformedPayload}
	}

	return json.Marshal(map[string]int{"recalculated_days": payload.Days})
}
