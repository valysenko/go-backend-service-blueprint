package operation

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Repository interface {
	Create(ctx context.Context, operation *Operation) error
	LockForUpdate(ctx context.Context, id uuid.UUID) (*Operation, error)
	ListPlannedBatch(ctx context.Context, plannedAt time.Time, afterUUID uuid.UUID, limit int) ([]*Operation, error)
	Update(ctx context.Context, operation *Operation) error
}
