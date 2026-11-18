package operation_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	appoperation "go-backend-service-blueprint/internal/app/operation"
	"go-backend-service-blueprint/internal/app/operation/mocks"
	txmocks "go-backend-service-blueprint/internal/app/tx/mocks"
	domainoperation "go-backend-service-blueprint/internal/domain/operation"
	operationmocks "go-backend-service-blueprint/internal/domain/operation/mocks"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRunnerPagesWithKeysetUntilTheBatchIsEmpty(t *testing.T) {
	plannedAt := time.Now().UTC()
	first := plannedOperation()
	second := plannedOperation()

	repo := operationmocks.NewMockRepository(t)
	repo.EXPECT().
		ListPlannedBatch(mock.Anything, plannedAt, uuid.Nil, 1).
		Return([]*domainoperation.Operation{first}, nil).
		Once()
	repo.EXPECT().
		ListPlannedBatch(mock.Anything, plannedAt, first.UUID, 1).
		Return([]*domainoperation.Operation{second}, nil).
		Once()
	repo.EXPECT().
		ListPlannedBatch(mock.Anything, plannedAt, second.UUID, 1).
		Return(nil, nil).
		Once()

	for _, op := range []*domainoperation.Operation{first, second} {
		repo.EXPECT().LockForUpdate(mock.Anything, op.UUID).Return(op, nil).Once()
		repo.EXPECT().Update(mock.Anything, op).Return(nil).Once()
	}

	manager := txmocks.NewMockManager(t)
	manager.EXPECT().
		WithinTx(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		}).
		Times(2)

	executor := mocks.NewMockExecutor(t)
	executor.EXPECT().Execute(mock.Anything, mock.Anything).Return(json.RawMessage(`{}`), nil).Times(2)

	registry := appoperation.NewRegistry(map[domainoperation.OperationType]appoperation.Executor{
		domainoperation.OperationTypeRecalculateStats: executor,
	})

	metrics := mocks.NewMockCronMetrics(t)
	metrics.EXPECT().
		IncrementProcessed(domainoperation.OperationTypeRecalculateStats.String(), appoperation.OutcomeCompleted).
		Times(2)
	metrics.EXPECT().ObserveRunDuration(mock.Anything, mock.Anything).Once()
	metrics.EXPECT().Push().Return(nil).Once()

	runner := appoperation.NewRunner(
		repo,
		appoperation.NewProcessor(repo, registry, manager, discard),
		metrics,
		discard,
		1,
	)

	summary, err := runner.Run(context.Background(), plannedAt)

	require.NoError(t, err)
	assert.Equal(t, 2, summary.Completed)
	assert.Equal(t, 2, summary.Total())
}

func TestRunnerCountsAFailingRowAndKeepsGoing(t *testing.T) {
	plannedAt := time.Now().UTC()
	broken := plannedOperation()
	healthy := plannedOperation()

	repo := operationmocks.NewMockRepository(t)
	repo.EXPECT().
		ListPlannedBatch(mock.Anything, plannedAt, uuid.Nil, 10).
		Return([]*domainoperation.Operation{broken, healthy}, nil).
		Once()
	repo.EXPECT().
		ListPlannedBatch(mock.Anything, plannedAt, healthy.UUID, 10).
		Return(nil, nil).
		Once()

	repo.EXPECT().LockForUpdate(mock.Anything, broken.UUID).Return(nil, domainoperation.ErrNotFound).Once()
	repo.EXPECT().LockForUpdate(mock.Anything, healthy.UUID).Return(healthy, nil).Once()
	repo.EXPECT().Update(mock.Anything, healthy).Return(nil).Once()

	manager := txmocks.NewMockManager(t)
	manager.EXPECT().
		WithinTx(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		}).
		Times(2)

	executor := mocks.NewMockExecutor(t)
	executor.EXPECT().Execute(mock.Anything, healthy).Return(json.RawMessage(`{}`), nil).Once()

	registry := appoperation.NewRegistry(map[domainoperation.OperationType]appoperation.Executor{
		domainoperation.OperationTypeRecalculateStats: executor,
	})

	metrics := mocks.NewMockCronMetrics(t)
	metrics.EXPECT().IncrementProcessed(mock.Anything, appoperation.OutcomeRecoverable).Once()
	metrics.EXPECT().IncrementProcessed(mock.Anything, appoperation.OutcomeCompleted).Once()
	metrics.EXPECT().ObserveRunDuration(mock.Anything, mock.Anything).Once()
	metrics.EXPECT().Push().Return(nil).Once()

	runner := appoperation.NewRunner(
		repo,
		appoperation.NewProcessor(repo, registry, manager, discard),
		metrics,
		discard,
		10,
	)

	summary, err := runner.Run(context.Background(), plannedAt)

	require.NoError(t, err)
	assert.Equal(t, 1, summary.Completed)
	assert.Equal(t, 1, summary.Recoverable)
}
