package user_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	appuser "go-backend-service-blueprint/internal/app/user"
	"go-backend-service-blueprint/internal/app/user/mocks"
	domainuser "go-backend-service-blueprint/internal/domain/user"
	domainmocks "go-backend-service-blueprint/internal/domain/user/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestCreateRejectsWhenRateLimited(t *testing.T) {
	repository := domainmocks.NewMockRepository(t)
	limiter := mocks.NewMockRateLimiter(t)
	limiter.EXPECT().
		Allow(mock.Anything, "user_create:10.0.0.1", 5, time.Minute).
		Return(false, 30*time.Second, nil).
		Once()

	_, err := appuser.NewUseCases(repository, limiter, 5, discard).
		Create(context.Background(), appuser.CreateCommand{Email: "jane@example.com", FullName: "Jane", ClientIP: "10.0.0.1"})

	var rateLimited *appuser.RateLimitedError
	require.ErrorAs(t, err, &rateLimited)
	assert.Equal(t, 30*time.Second, rateLimited.RetryAfter)
}

func TestCreateFailsOpenWhenLimiterErrors(t *testing.T) {
	repository := domainmocks.NewMockRepository(t)
	repository.EXPECT().Create(mock.Anything, mock.Anything).Return(nil).Once()

	limiter := mocks.NewMockRateLimiter(t)
	limiter.EXPECT().
		Allow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(false, 0, errors.New("redis is down")).
		Once()

	created, err := appuser.NewUseCases(repository, limiter, 5, discard).
		Create(context.Background(), appuser.CreateCommand{Email: "Jane@Example.com", FullName: "Jane"})

	require.NoError(t, err)
	assert.Equal(t, "jane@example.com", created.Email)
}

func TestCreateSkipsLimiterWhenDisabled(t *testing.T) {
	repository := domainmocks.NewMockRepository(t)
	repository.EXPECT().Create(mock.Anything, mock.Anything).Return(nil).Once()

	limiter := mocks.NewMockRateLimiter(t)

	_, err := appuser.NewUseCases(repository, limiter, 0, discard).
		Create(context.Background(), appuser.CreateCommand{Email: "jane@example.com", FullName: "Jane"})

	require.NoError(t, err)
}

func TestCreateRejectsInvalidInputBeforeTouchingTheRepository(t *testing.T) {
	repository := domainmocks.NewMockRepository(t)
	limiter := mocks.NewMockRateLimiter(t)
	limiter.EXPECT().Allow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, 0, nil).Once()

	_, err := appuser.NewUseCases(repository, limiter, 5, discard).
		Create(context.Background(), appuser.CreateCommand{Email: "  ", FullName: "Jane"})

	require.ErrorIs(t, err, domainuser.ErrInvalid)
}

func TestListClampsPageAndLimit(t *testing.T) {
	repository := domainmocks.NewMockRepository(t)
	repository.EXPECT().
		List(mock.Anything, domainuser.ListFilter{Limit: 100, Offset: 0}).
		Return(nil, nil).
		Once()

	result, err := appuser.NewUseCases(repository, appuser.NewUnlimited(), 5, discard).
		List(context.Background(), appuser.ListQuery{Page: 0, Limit: 500})

	require.NoError(t, err)
	assert.Equal(t, 1, result.Page)
	assert.Equal(t, 100, result.Limit)
}

func TestListAppliesOffsetForLaterPages(t *testing.T) {
	repository := domainmocks.NewMockRepository(t)
	repository.EXPECT().
		List(mock.Anything, domainuser.ListFilter{Limit: 10, Offset: 20}).
		Return([]domainuser.User{}, nil).
		Once()

	result, err := appuser.NewUseCases(repository, appuser.NewUnlimited(), 5, discard).
		List(context.Background(), appuser.ListQuery{Page: 3, Limit: 10})

	require.NoError(t, err)
	assert.Equal(t, 3, result.Page)
	assert.Equal(t, 10, result.Limit)
}
