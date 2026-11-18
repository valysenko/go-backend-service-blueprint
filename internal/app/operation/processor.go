package operation

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go-backend-service-blueprint/internal/app/tx"
	"go-backend-service-blueprint/internal/domain/operation"

	"github.com/google/uuid"
)

const (
	OutcomeCompleted   = "completed"
	OutcomeFailed      = "failed"
	OutcomeRecoverable = "recoverable"
	OutcomeSkipped     = "skipped"
)

type Processor struct {
	repo     operation.Repository
	registry *Registry
	tx       tx.Manager
	logger   *slog.Logger
}

func NewProcessor(repo operation.Repository, registry *Registry, manager tx.Manager, logger *slog.Logger) *Processor {
	return &Processor{repo: repo, registry: registry, tx: manager, logger: logger}
}

func (p *Processor) Process(ctx context.Context, id uuid.UUID) (string, error) {
	outcome := OutcomeSkipped

	txErr := p.tx.WithinTx(ctx, func(ctx context.Context) error {
		op, err := p.repo.LockForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if !op.Status.IsPlanned() {
			p.logger.InfoContext(ctx, "operation already settled, skipping",
				"operation_uuid", op.UUID, "status", op.Status.String())

			return nil
		}

		now := time.Now().UTC()
		result, execErr := p.execute(ctx, op)
		if execErr == nil {
			op.Complete(result, now)
			outcome = OutcomeCompleted

			return p.repo.Update(ctx, op)
		}

		var failed *operation.FailedError
		if errors.As(execErr, &failed) {
			p.logger.ErrorContext(ctx, "operation failed permanently",
				"operation_uuid", op.UUID, "operation_type", op.OperationType.String(), "reason", failed.Reason)
			op.Fail(failed.Reason, now)
			outcome = OutcomeFailed

			return p.repo.Update(ctx, op)
		}

		var recoverable *operation.RecoverableError
		if !errors.As(execErr, &recoverable) {
			p.logger.ErrorContext(ctx, "operation returned an unknown error class, treating as recoverable",
				"operation_uuid", op.UUID, "error", execErr)
		}
		p.logger.WarnContext(ctx, "operation recoverable, staying planned for the next tick",
			"operation_uuid", op.UUID, "operation_type", op.OperationType.String(), "error", execErr)
		outcome = OutcomeRecoverable

		return nil
	})
	if txErr != nil {
		// A contended row aborts its transaction, so the skip is recognised here
		// rather than inside the callback: the rollback is what has to happen.
		if errors.Is(txErr, operation.ErrLocked) {
			p.logger.InfoContext(ctx, "operation is held by another worker, skipping", "operation_uuid", id)

			return OutcomeSkipped, nil
		}

		return outcome, txErr
	}

	return outcome, nil
}

func (p *Processor) execute(ctx context.Context, op *operation.Operation) (result []byte, err error) {
	executor, err := p.registry.For(op.OperationType)
	if err != nil {
		return nil, err
	}

	return executor.Execute(ctx, op)
}
