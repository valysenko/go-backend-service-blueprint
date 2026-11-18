package operation

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"go-backend-service-blueprint/internal/domain/operation"

	"github.com/google/uuid"
)

type Summary struct {
	Completed   int
	Failed      int
	Recoverable int
	Skipped     int
}

func (s Summary) Total() int {
	return s.Completed + s.Failed + s.Recoverable + s.Skipped
}

type Runner struct {
	repo      operation.Repository
	processor *Processor
	metrics   CronMetrics
	logger    *slog.Logger
	batchSize int
}

func NewRunner(
	repo operation.Repository,
	processor *Processor,
	metrics CronMetrics,
	logger *slog.Logger,
	batchSize int,
) *Runner {
	return &Runner{
		repo:      repo,
		processor: processor,
		metrics:   metrics,
		logger:    logger,
		batchSize: batchSize,
	}
}

func (r *Runner) Run(ctx context.Context, plannedAt time.Time) (Summary, error) {
	startedAt := time.Now()
	runID := strconv.FormatInt(startedAt.Unix(), 10)
	afterUUID := uuid.Nil
	summary := Summary{}

	r.logger.InfoContext(ctx, "operations runner started", "run_id", runID, "planned_at", plannedAt)

	for {
		if ctx.Err() != nil {
			return summary, ctx.Err()
		}

		batch, err := r.repo.ListPlannedBatch(ctx, plannedAt, afterUUID, r.batchSize)
		if err != nil {
			return summary, fmt.Errorf("list planned operations: %w", err)
		}
		if len(batch) == 0 {
			break
		}

		for _, op := range batch {
			if ctx.Err() != nil {
				return summary, ctx.Err()
			}
			// might be reworked using worker pool
			r.processOne(ctx, op, &summary)
		}

		afterUUID = batch[len(batch)-1].UUID
	}

	r.logger.InfoContext(ctx, "operations runner finished",
		"run_id", runID,
		"completed", summary.Completed,
		"failed", summary.Failed,
		"recoverable", summary.Recoverable,
		"skipped", summary.Skipped,
	)

	r.metrics.ObserveRunDuration(time.Since(startedAt), runID)
	if err := r.metrics.Push(); err != nil {
		r.logger.WarnContext(ctx, "failed to push cron metrics", "error", err)
	}

	return summary, nil
}

// processOne never aborts the run: one broken row must not stop the batch, so a
// transaction error is counted as recoverable and the loop moves on.
func (r *Runner) processOne(ctx context.Context, op *operation.Operation, summary *Summary) {
	outcome, err := r.processor.Process(ctx, op.UUID)
	if err != nil {
		r.logger.ErrorContext(ctx, "failed to settle operation",
			"operation_uuid", op.UUID, "operation_type", op.OperationType.String(), "error", err)
		outcome = OutcomeRecoverable
	}

	switch outcome {
	case OutcomeCompleted:
		summary.Completed++
	case OutcomeFailed:
		summary.Failed++
	case OutcomeSkipped:
		summary.Skipped++
	default:
		summary.Recoverable++
	}

	r.metrics.IncrementProcessed(op.OperationType.String(), outcome)
}
