package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	domainoperation "go-backend-service-blueprint/internal/domain/operation"
	"go-backend-service-blueprint/pkg/database"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	operationColumns   = "uuid, operation_type, entity_id, status, payload, result, planned_at, created_at, updated_at"
	pgLockNotAvailable = "55P03"
)

type OperationRepository struct {
	database *database.AppDB
}

var _ domainoperation.Repository = (*OperationRepository)(nil)

func NewOperationRepository(db *database.AppDB) *OperationRepository {
	return &OperationRepository{database: db}
}

func (r *OperationRepository) querier(ctx context.Context) querier {
	return querierFrom(ctx, r.database.Postgresql)
}

func (r *OperationRepository) Create(ctx context.Context, op *domainoperation.Operation) error {
	query := `INSERT INTO operations (` + operationColumns + `)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	_, err := r.querier(ctx).Exec(ctx, query,
		op.UUID, op.OperationType, op.EntityID, op.Status, op.Payload, op.Result,
		op.PlannedAt, op.CreatedAt, op.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert operation %s: %w", op.UUID, err)
	}

	return nil
}

// LockForUpdate refuses to wait for a row another worker already holds
func (r *OperationRepository) LockForUpdate(ctx context.Context, id uuid.UUID) (*domainoperation.Operation, error) {
	query := `SELECT ` + operationColumns + ` FROM operations WHERE uuid = $1 FOR UPDATE NOWAIT`

	op, err := r.scanFirst(ctx, query, id)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgLockNotAvailable {
			return nil, fmt.Errorf("%w: uuid=%s", domainoperation.ErrLocked, id)
		}

		return nil, err
	}
	if op == nil {
		return nil, fmt.Errorf("%w: uuid=%s", domainoperation.ErrNotFound, id)
	}

	return op, nil
}

func (r *OperationRepository) scanFirst(ctx context.Context, query string, id uuid.UUID) (*domainoperation.Operation, error) {
	rows, err := r.querier(ctx).Query(ctx, query, id)
	if err != nil {
		return nil, fmt.Errorf("select operation %s: %w", id, err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("select operation %s: %w", id, err)
		}

		return nil, nil
	}

	return scanOperation(rows)
}

func (r *OperationRepository) ListPlannedBatch(
	ctx context.Context,
	plannedAt time.Time,
	afterUUID uuid.UUID,
	limit int,
) ([]*domainoperation.Operation, error) {
	query := `SELECT ` + operationColumns + ` FROM operations
		WHERE status = $1
			AND planned_at <= $2
			AND uuid > $3
		ORDER BY uuid
		LIMIT $4`

	rows, err := r.querier(ctx).Query(ctx, query, domainoperation.StatusPlanned, plannedAt, afterUUID, limit)
	if err != nil {
		return nil, fmt.Errorf("select planned operations: %w", err)
	}
	defer rows.Close()

	operations := make([]*domainoperation.Operation, 0, limit)
	for rows.Next() {
		op, scanErr := scanOperation(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		operations = append(operations, op)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate planned operations: %w", err)
	}

	return operations, nil
}

func (r *OperationRepository) Update(ctx context.Context, op *domainoperation.Operation) error {
	query := `UPDATE operations
		SET operation_type = $2, entity_id = $3, status = $4, payload = $5, result = $6,
			planned_at = $7, updated_at = $8
		WHERE uuid = $1`

	tag, err := r.querier(ctx).Exec(ctx, query,
		op.UUID, op.OperationType, op.EntityID, op.Status, op.Payload, op.Result, op.PlannedAt, op.UpdatedAt)
	if err != nil {
		return fmt.Errorf("update operation %s: %w", op.UUID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: uuid=%s", domainoperation.ErrNotFound, op.UUID)
	}

	return nil
}

func scanOperation(rows pgx.Rows) (*domainoperation.Operation, error) {
	var op domainoperation.Operation
	if err := rows.Scan(
		&op.UUID,
		&op.OperationType,
		&op.EntityID,
		&op.Status,
		&op.Payload,
		&op.Result,
		&op.PlannedAt,
		&op.CreatedAt,
		&op.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("scan operation: %w", err)
	}

	return &op, nil
}
