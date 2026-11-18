package user

import "errors"

var (
	ErrNotFound      = errors.New("user: not found")
	ErrAlreadyExists = errors.New("user: already exists")
	ErrInvalid       = errors.New("user: invalid")
)
