package reader

import "fmt"

// skip retries; route to DL
type UnrecoverableError struct {
	Context string
	Err     error
}

func NewUnrecoverableError(err error, info string) *UnrecoverableError {
	return &UnrecoverableError{
		Context: info,
		Err:     err,
	}
}

func (e *UnrecoverableError) Error() string {
	return fmt.Sprintf("%s: %v", e.Context, e.Err)
}

func (e *UnrecoverableError) Unwrap() error {
	return e.Err
}

// skip retries and DL — message will be ignored and committed
type DiscardError struct {
	Context string
	Err     error
}

func NewDiscardError(err error, info string) *DiscardError {
	return &DiscardError{
		Context: info,
		Err:     err,
	}
}

func (e *DiscardError) Error() string {
	return fmt.Sprintf("%s: %v", e.Context, e.Err)
}

func (e *DiscardError) Unwrap() error {
	return e.Err
}

// ReaderFailure marks an error as fatal: skip retries, skip DL, propagate to
// the reader so the pod restarts.
//
// Returnable from any layer:
//   - handlers tag their own infra errors (Redis, DL writer);
//   - domain processors might return it when a dependency is fully unreachable.
type ReaderFailure struct {
	Context string
	Err     error
}

func NewReaderFailure(err error, info string) *ReaderFailure {
	return &ReaderFailure{
		Context: info,
		Err:     err,
	}
}

func (e *ReaderFailure) Error() string {
	return fmt.Sprintf("%s: %v", e.Context, e.Err)
}

func (e *ReaderFailure) Unwrap() error {
	return e.Err
}
