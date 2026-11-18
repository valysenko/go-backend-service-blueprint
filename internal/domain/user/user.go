package user

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type User struct {
	UUID      uuid.UUID
	Email     string
	FullName  string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func New(email, fullName string, now time.Time) (*User, error) {
	email = NormalizeEmail(email)
	fullName = strings.TrimSpace(fullName)

	if email == "" {
		return nil, fmt.Errorf("%w: email must not be empty", ErrInvalid)
	}
	if fullName == "" {
		return nil, fmt.Errorf("%w: full name must not be empty", ErrInvalid)
	}

	return &User{
		UUID:      uuid.New(),
		Email:     email,
		FullName:  fullName,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func NormalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}
