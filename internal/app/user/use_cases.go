package user

import (
	"context"
	"log/slog"
	"time"

	"go-backend-service-blueprint/internal/app/tx"
	domainoperation "go-backend-service-blueprint/internal/domain/operation"
	domainuser "go-backend-service-blueprint/internal/domain/user"

	"github.com/google/uuid"
)

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

const createRateLimitWindow = time.Minute

type CreateCommand struct {
	Email    string
	FullName string
	ClientIP string
}

type ListQuery struct {
	Page  int
	Limit int
}

type ListResult struct {
	Items []domainuser.User
	Page  int
	Limit int
}

type UseCases struct {
	repo             domainuser.Repository
	operations       domainoperation.Repository
	tx               tx.Manager
	limiter          RateLimiter
	createsPerMinute int
	logger           *slog.Logger
}

func NewUseCases(
	repo domainuser.Repository,
	operations domainoperation.Repository,
	manager tx.Manager,
	limiter RateLimiter,
	createsPerMinute int,
	logger *slog.Logger,
) *UseCases {
	return &UseCases{
		repo:             repo,
		operations:       operations,
		tx:               manager,
		limiter:          limiter,
		createsPerMinute: createsPerMinute,
		logger:           logger,
	}
}

func (uc *UseCases) Create(ctx context.Context, cmd CreateCommand) (*domainuser.User, error) {
	if err := uc.allowCreate(ctx, cmd.ClientIP); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	created, err := domainuser.New(cmd.Email, cmd.FullName, now)
	if err != nil {
		return nil, err
	}

	err = uc.tx.WithinTx(ctx, func(ctx context.Context) error {
		if err := uc.repo.Create(ctx, created); err != nil {
			return err
		}

		welcome := domainoperation.New(
			domainoperation.OperationTypeSendWelcomeEmail,
			created.UUID,
			nil,
			now,
			now,
		)

		return uc.operations.Create(ctx, welcome)
	})
	if err != nil {
		return nil, err
	}

	uc.logger.InfoContext(ctx, "user created", "user_uuid", created.UUID)

	return created, nil
}

// allowCreate fails open: a Redis outage must not take the endpoint down with
// it, so a limiter error is logged and the request proceeds.
func (uc *UseCases) allowCreate(ctx context.Context, clientIP string) error {
	if uc.createsPerMinute <= 0 {
		return nil
	}

	allowed, retryAfter, err := uc.limiter.Allow(ctx, "user_create:"+clientIP, uc.createsPerMinute, createRateLimitWindow)
	if err != nil {
		uc.logger.WarnContext(ctx, "rate limiter unavailable, allowing request", "error", err)

		return nil
	}
	if !allowed {
		return &RateLimitedError{RetryAfter: retryAfter}
	}

	return nil
}

func (uc *UseCases) Get(ctx context.Context, id uuid.UUID) (*domainuser.User, error) {
	return uc.repo.GetByUUID(ctx, id)
}

func (uc *UseCases) List(ctx context.Context, query ListQuery) (*ListResult, error) {
	page := max(query.Page, 1)

	limit := query.Limit
	switch {
	case limit <= 0:
		limit = defaultPageSize
	case limit > maxPageSize:
		limit = maxPageSize
	}

	items, err := uc.repo.List(ctx, domainuser.ListFilter{
		Limit:  limit,
		Offset: (page - 1) * limit,
	})
	if err != nil {
		return nil, err
	}

	return &ListResult{Items: items, Page: page, Limit: limit}, nil
}
