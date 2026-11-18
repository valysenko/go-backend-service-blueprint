package user_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	txmocks "go-backend-service-blueprint/internal/app/tx/mocks"
	appuser "go-backend-service-blueprint/internal/app/user"
	"go-backend-service-blueprint/internal/app/user/mocks"
	domainoperation "go-backend-service-blueprint/internal/domain/operation"
	operationmocks "go-backend-service-blueprint/internal/domain/operation/mocks"
	domainuser "go-backend-service-blueprint/internal/domain/user"
	usermocks "go-backend-service-blueprint/internal/domain/user/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type deps struct {
	users      *usermocks.MockRepository
	operations *operationmocks.MockRepository
	manager    *txmocks.MockManager
	limiter    *mocks.MockRateLimiter
}

func newDeps(t *testing.T) deps {
	t.Helper()

	return deps{
		users:      usermocks.NewMockRepository(t),
		operations: operationmocks.NewMockRepository(t),
		manager:    txmocks.NewMockManager(t),
		limiter:    mocks.NewMockRateLimiter(t),
	}
}

func (d deps) useCases(perMinute int) *appuser.UseCases {
	return appuser.NewUseCases(d.users, d.operations, d.manager, d.limiter, perMinute, discard)
}

// expectTransaction makes the mocked manager run the callback it is given, so
// the test exercises the real body of the transaction.
func (d deps) expectTransaction() {
	d.manager.EXPECT().
		WithinTx(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		}).
		Once()
}

func TestCreateSchedulesWelcomeOperationInTheSameTransaction(t *testing.T) {
	d := newDeps(t)
	d.limiter.EXPECT().Allow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, 0, nil).Once()
	d.expectTransaction()
	d.users.EXPECT().Create(mock.Anything, mock.Anything).Return(nil).Once()

	var scheduled *domainoperation.Operation
	d.operations.EXPECT().
		Create(mock.Anything, mock.Anything).
		Run(func(_ context.Context, op *domainoperation.Operation) { scheduled = op }).
		Return(nil).
		Once()

	created, err := d.useCases(5).
		Create(context.Background(), appuser.CreateCommand{Email: "Jane@Example.com", FullName: "Jane"})

	require.NoError(t, err)
	require.NotNil(t, scheduled)
	assert.Equal(t, domainoperation.OperationTypeSendWelcomeEmail, scheduled.OperationType)
	assert.Equal(t, created.UUID, scheduled.EntityID)
	assert.Equal(t, domainoperation.StatusPlanned, scheduled.Status)
}

func TestCreateReturnsTheOperationError(t *testing.T) {
	d := newDeps(t)
	d.limiter.EXPECT().Allow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, 0, nil).Once()
	d.expectTransaction()
	d.users.EXPECT().Create(mock.Anything, mock.Anything).Return(nil).Once()
	d.operations.EXPECT().Create(mock.Anything, mock.Anything).Return(errors.New("insert failed")).Once()

	_, err := d.useCases(5).
		Create(context.Background(), appuser.CreateCommand{Email: "jane@example.com", FullName: "Jane"})

	require.Error(t, err)
}

func TestCreateRejectsWhenRateLimited(t *testing.T) {
	d := newDeps(t)
	d.limiter.EXPECT().
		Allow(mock.Anything, "user_create:10.0.0.1", 5, time.Minute).
		Return(false, 30*time.Second, nil).
		Once()

	_, err := d.useCases(5).
		Create(context.Background(), appuser.CreateCommand{Email: "jane@example.com", FullName: "Jane", ClientIP: "10.0.0.1"})

	var rateLimited *appuser.RateLimitedError
	require.ErrorAs(t, err, &rateLimited)
	assert.Equal(t, 30*time.Second, rateLimited.RetryAfter)
}

func TestCreateFailsOpenWhenLimiterErrors(t *testing.T) {
	d := newDeps(t)
	d.limiter.EXPECT().
		Allow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(false, 0, errors.New("redis is down")).
		Once()
	d.expectTransaction()
	d.users.EXPECT().Create(mock.Anything, mock.Anything).Return(nil).Once()
	d.operations.EXPECT().Create(mock.Anything, mock.Anything).Return(nil).Once()

	created, err := d.useCases(5).
		Create(context.Background(), appuser.CreateCommand{Email: "Jane@Example.com", FullName: "Jane"})

	require.NoError(t, err)
	assert.Equal(t, "jane@example.com", created.Email)
}

func TestCreateSkipsLimiterWhenDisabled(t *testing.T) {
	d := newDeps(t)
	d.expectTransaction()
	d.users.EXPECT().Create(mock.Anything, mock.Anything).Return(nil).Once()
	d.operations.EXPECT().Create(mock.Anything, mock.Anything).Return(nil).Once()

	_, err := d.useCases(0).
		Create(context.Background(), appuser.CreateCommand{Email: "jane@example.com", FullName: "Jane"})

	require.NoError(t, err)
}

func TestCreateRejectsInvalidInputBeforeOpeningATransaction(t *testing.T) {
	d := newDeps(t)
	d.limiter.EXPECT().Allow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, 0, nil).Once()

	_, err := d.useCases(5).
		Create(context.Background(), appuser.CreateCommand{Email: "  ", FullName: "Jane"})

	require.ErrorIs(t, err, domainuser.ErrInvalid)
}

func TestListClampsPageAndLimit(t *testing.T) {
	d := newDeps(t)
	d.users.EXPECT().
		List(mock.Anything, domainuser.ListFilter{Limit: 100, Offset: 0}).
		Return(nil, nil).
		Once()

	result, err := appuser.NewUseCases(d.users, d.operations, d.manager, appuser.NewUnlimited(), 5, discard).
		List(context.Background(), appuser.ListQuery{Page: 0, Limit: 500})

	require.NoError(t, err)
	assert.Equal(t, 1, result.Page)
	assert.Equal(t, 100, result.Limit)
}

func TestListAppliesOffsetForLaterPages(t *testing.T) {
	d := newDeps(t)
	d.users.EXPECT().
		List(mock.Anything, domainuser.ListFilter{Limit: 10, Offset: 20}).
		Return([]domainuser.User{}, nil).
		Once()

	result, err := appuser.NewUseCases(d.users, d.operations, d.manager, appuser.NewUnlimited(), 5, discard).
		List(context.Background(), appuser.ListQuery{Page: 3, Limit: 10})

	require.NoError(t, err)
	assert.Equal(t, 3, result.Page)
	assert.Equal(t, 10, result.Limit)
}
