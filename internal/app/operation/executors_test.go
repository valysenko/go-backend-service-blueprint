package operation_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	appoperation "go-backend-service-blueprint/internal/app/operation"
	"go-backend-service-blueprint/internal/app/operation/mocks"
	domainoperation "go-backend-service-blueprint/internal/domain/operation"
	domainuser "go-backend-service-blueprint/internal/domain/user"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func welcomeOperation(entityID uuid.UUID) *domainoperation.Operation {
	now := time.Now().UTC()

	return domainoperation.New(domainoperation.OperationTypeSendWelcomeEmail, entityID, nil, now, now)
}

func TestWelcomeEmailExecutorSendsToTheStoredAddress(t *testing.T) {
	recipient := &domainuser.User{UUID: uuid.New(), Email: "jane@example.com", FullName: "Jane"}

	users := mocks.NewMockUserSource(t)
	users.EXPECT().GetByUUID(mock.Anything, recipient.UUID).Return(recipient, nil).Once()

	sender := mocks.NewMockEmailSender(t)
	sender.EXPECT().SendWelcome(mock.Anything, recipient.Email, recipient.FullName).Return(nil).Once()

	result, err := appoperation.NewWelcomeEmailExecutor(users, sender).
		Execute(context.Background(), welcomeOperation(recipient.UUID))

	require.NoError(t, err)
	assert.JSONEq(t, `{"sent_to":"jane@example.com"}`, string(result))
}

func TestWelcomeEmailExecutorFailsTerminallyForAMissingUser(t *testing.T) {
	entityID := uuid.New()

	users := mocks.NewMockUserSource(t)
	users.EXPECT().GetByUUID(mock.Anything, entityID).Return(nil, domainuser.ErrNotFound).Once()

	_, err := appoperation.NewWelcomeEmailExecutor(users, mocks.NewMockEmailSender(t)).
		Execute(context.Background(), welcomeOperation(entityID))

	var failed *domainoperation.FailedError
	require.ErrorAs(t, err, &failed)
	assert.Equal(t, domainoperation.ReasonEntityNotFound, failed.Reason)
}

func TestWelcomeEmailExecutorRetriesOnSenderFailure(t *testing.T) {
	recipient := &domainuser.User{UUID: uuid.New(), Email: "jane@example.com"}

	users := mocks.NewMockUserSource(t)
	users.EXPECT().GetByUUID(mock.Anything, recipient.UUID).Return(recipient, nil).Once()

	sender := mocks.NewMockEmailSender(t)
	sender.EXPECT().SendWelcome(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("smtp down")).Once()

	_, err := appoperation.NewWelcomeEmailExecutor(users, sender).
		Execute(context.Background(), welcomeOperation(recipient.UUID))

	var recoverable *domainoperation.RecoverableError
	require.ErrorAs(t, err, &recoverable)
}

func TestWelcomeEmailExecutorRetriesOnStorageFailure(t *testing.T) {
	entityID := uuid.New()

	users := mocks.NewMockUserSource(t)
	users.EXPECT().GetByUUID(mock.Anything, entityID).Return(nil, errors.New("connection refused")).Once()

	_, err := appoperation.NewWelcomeEmailExecutor(users, mocks.NewMockEmailSender(t)).
		Execute(context.Background(), welcomeOperation(entityID))

	var recoverable *domainoperation.RecoverableError
	require.ErrorAs(t, err, &recoverable)
}

func TestRecalculateStatsExecutorEchoesTheWindow(t *testing.T) {
	now := time.Now().UTC()
	op := domainoperation.New(
		domainoperation.OperationTypeRecalculateStats,
		uuid.New(),
		json.RawMessage(`{"days":30}`),
		now,
		now,
	)

	result, err := appoperation.NewRecalculateStatsExecutor().Execute(context.Background(), op)

	require.NoError(t, err)
	assert.JSONEq(t, `{"recalculated_days":30}`, string(result))
}

func TestRecalculateStatsExecutorStopsOnACancelledContext(t *testing.T) {
	now := time.Now().UTC()
	op := domainoperation.New(
		domainoperation.OperationTypeRecalculateStats,
		uuid.New(),
		json.RawMessage(`{"days":30}`),
		now,
		now,
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := appoperation.NewRecalculateStatsExecutor().Execute(ctx, op)

	var recoverable *domainoperation.RecoverableError
	require.ErrorAs(t, err, &recoverable)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestRecalculateStatsExecutorRejectsAnEmptyWindow(t *testing.T) {
	now := time.Now().UTC()
	op := domainoperation.New(domainoperation.OperationTypeRecalculateStats, uuid.New(), nil, now, now)

	_, err := appoperation.NewRecalculateStatsExecutor().Execute(context.Background(), op)

	var failed *domainoperation.FailedError
	require.ErrorAs(t, err, &failed)
	assert.Equal(t, domainoperation.ReasonMalformedPayload, failed.Reason)
}

func TestRegistryReportsUnknownTypeAsTerminal(t *testing.T) {
	_, err := appoperation.NewRegistry(nil).For(domainoperation.OperationTypeUnknown)

	var failed *domainoperation.FailedError
	require.ErrorAs(t, err, &failed)
	assert.Equal(t, domainoperation.ReasonUnknownOperationType, failed.Reason)
}
