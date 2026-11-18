package operation

import (
	"context"
	"time"

	domainuser "go-backend-service-blueprint/internal/domain/user"

	"github.com/google/uuid"
)

type UserSource interface {
	GetByUUID(ctx context.Context, id uuid.UUID) (*domainuser.User, error)
}

type EmailSender interface {
	SendWelcome(ctx context.Context, email, fullName string) error
}

type CronMetrics interface {
	ObserveRunDuration(duration time.Duration, runID string)
	IncrementProcessed(operationType, outcome string)
	Push() error
}
