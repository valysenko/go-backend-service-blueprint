package user

import (
	"context"

	"github.com/google/uuid"
)

type ListFilter struct {
	Limit  int
	Offset int
}

type Repository interface {
	Create(ctx context.Context, user *User) error
	GetByUUID(ctx context.Context, id uuid.UUID) (*User, error)
	List(ctx context.Context, filter ListFilter) ([]User, error)
}
