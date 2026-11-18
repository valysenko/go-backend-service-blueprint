package operation_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
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

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func plannedOperation() *domainoperation.Operation {
	return domainoperation.New(
		domainoperation.OperationTypeRecalculateStats,
		uuid.New(),
		json.RawMessage(`{"days":7}`),
		time.Now().UTC(),
		time.Now().UTC(),
	)
}

func runningManager(t *testing.T) *txmocks.MockManager {
	t.Helper()

	manager := txmocks.NewMockManager(t)
	manager.EXPECT().
		WithinTx(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		}).
		Once()

	return manager
}

func TestProcessCompletesTheOperation(t *testing.T) {
	op := plannedOperation()

	repo := operationmocks.NewMockRepository(t)
	repo.EXPECT().LockForUpdate(mock.Anything, op.UUID).Return(op, nil).Once()

	var settled *domainoperation.Operation
	repo.EXPECT().
		Update(mock.Anything, mock.Anything).
		Run(func(_ context.Context, updated *domainoperation.Operation) { settled = updated }).
		Return(nil).
		Once()

	executor := mocks.NewMockExecutor(t)
	executor.EXPECT().Execute(mock.Anything, op).Return(json.RawMessage(`{"ok":true}`), nil).Once()

	registry := appoperation.NewRegistry(map[domainoperation.OperationType]appoperation.Executor{
		domainoperation.OperationTypeRecalculateStats: executor,
	})

	outcome, err := appoperation.NewProcessor(repo, registry, runningManager(t), discard).
		Process(context.Background(), op.UUID)

	require.NoError(t, err)
	assert.Equal(t, appoperation.OutcomeCompleted, outcome)
	require.NotNil(t, settled)
	assert.Equal(t, domainoperation.StatusCompleted, settled.Status)
	assert.Contains(t, string(settled.Result), "completed_at")
}

func TestProcessFailsTerminallyOnFailedError(t *testing.T) {
	op := plannedOperation()

	repo := operationmocks.NewMockRepository(t)
	repo.EXPECT().LockForUpdate(mock.Anything, op.UUID).Return(op, nil).Once()

	var settled *domainoperation.Operation
	repo.EXPECT().
		Update(mock.Anything, mock.Anything).
		Run(func(_ context.Context, updated *domainoperation.Operation) { settled = updated }).
		Return(nil).
		Once()

	executor := mocks.NewMockExecutor(t)
	executor.EXPECT().
		Execute(mock.Anything, op).
		Return(nil, &domainoperation.FailedError{Reason: domainoperation.ReasonEntityNotFound}).
		Once()

	registry := appoperation.NewRegistry(map[domainoperation.OperationType]appoperation.Executor{
		domainoperation.OperationTypeRecalculateStats: executor,
	})

	outcome, err := appoperation.NewProcessor(repo, registry, runningManager(t), discard).
		Process(context.Background(), op.UUID)

	require.NoError(t, err)
	assert.Equal(t, appoperation.OutcomeFailed, outcome)
	require.NotNil(t, settled)
	assert.Equal(t, domainoperation.StatusFailed, settled.Status)
	assert.Contains(t, string(settled.Result), domainoperation.ReasonEntityNotFound)
}

func TestProcessLeavesRowPlannedOnRecoverableError(t *testing.T) {
	op := plannedOperation()

	repo := operationmocks.NewMockRepository(t)
	repo.EXPECT().LockForUpdate(mock.Anything, op.UUID).Return(op, nil).Once()

	executor := mocks.NewMockExecutor(t)
	executor.EXPECT().
		Execute(mock.Anything, op).
		Return(nil, &domainoperation.RecoverableError{Cause: errors.New("upstream timeout")}).
		Once()

	registry := appoperation.NewRegistry(map[domainoperation.OperationType]appoperation.Executor{
		domainoperation.OperationTypeRecalculateStats: executor,
	})

	outcome, err := appoperation.NewProcessor(repo, registry, runningManager(t), discard).
		Process(context.Background(), op.UUID)

	require.NoError(t, err)
	assert.Equal(t, appoperation.OutcomeRecoverable, outcome)
	assert.Equal(t, domainoperation.StatusPlanned, op.Status)
}

func TestProcessTreatsUnknownErrorClassAsRecoverable(t *testing.T) {
	op := plannedOperation()

	repo := operationmocks.NewMockRepository(t)
	repo.EXPECT().LockForUpdate(mock.Anything, op.UUID).Return(op, nil).Once()

	executor := mocks.NewMockExecutor(t)
	executor.EXPECT().Execute(mock.Anything, op).Return(nil, errors.New("boom")).Once()

	registry := appoperation.NewRegistry(map[domainoperation.OperationType]appoperation.Executor{
		domainoperation.OperationTypeRecalculateStats: executor,
	})

	outcome, err := appoperation.NewProcessor(repo, registry, runningManager(t), discard).
		Process(context.Background(), op.UUID)

	require.NoError(t, err)
	assert.Equal(t, appoperation.OutcomeRecoverable, outcome)
	assert.Equal(t, domainoperation.StatusPlanned, op.Status)
}

func TestProcessFailsOperationWithoutRegisteredExecutor(t *testing.T) {
	op := plannedOperation()

	repo := operationmocks.NewMockRepository(t)
	repo.EXPECT().LockForUpdate(mock.Anything, op.UUID).Return(op, nil).Once()

	var settled *domainoperation.Operation
	repo.EXPECT().
		Update(mock.Anything, mock.Anything).
		Run(func(_ context.Context, updated *domainoperation.Operation) { settled = updated }).
		Return(nil).
		Once()

	registry := appoperation.NewRegistry(nil)

	outcome, err := appoperation.NewProcessor(repo, registry, runningManager(t), discard).
		Process(context.Background(), op.UUID)

	require.NoError(t, err)
	assert.Equal(t, appoperation.OutcomeFailed, outcome)
	require.NotNil(t, settled)
	assert.Contains(t, string(settled.Result), domainoperation.ReasonUnknownOperationType)
}

func TestProcessSkipsAnOperationHeldByAnotherWorker(t *testing.T) {
	id := uuid.New()

	repo := operationmocks.NewMockRepository(t)
	repo.EXPECT().LockForUpdate(mock.Anything, id).Return(nil, domainoperation.ErrLocked).Once()

	outcome, err := appoperation.NewProcessor(repo, appoperation.NewRegistry(nil), runningManager(t), discard).
		Process(context.Background(), id)

	require.NoError(t, err)
	assert.Equal(t, appoperation.OutcomeSkipped, outcome)
}

func TestProcessSkipsAlreadySettledOperation(t *testing.T) {
	op := plannedOperation()
	op.Complete(nil, time.Now().UTC())

	repo := operationmocks.NewMockRepository(t)
	repo.EXPECT().LockForUpdate(mock.Anything, op.UUID).Return(op, nil).Once()

	registry := appoperation.NewRegistry(map[domainoperation.OperationType]appoperation.Executor{
		domainoperation.OperationTypeRecalculateStats: mocks.NewMockExecutor(t),
	})

	outcome, err := appoperation.NewProcessor(repo, registry, runningManager(t), discard).
		Process(context.Background(), op.UUID)

	require.NoError(t, err)
	assert.Equal(t, appoperation.OutcomeSkipped, outcome)
}
