package operation

import (
	"errors"
	"fmt"
)

const (
	ReasonUnknownOperationType = "unknown_operation_type"
	ReasonEntityNotFound       = "entity_not_found"
	ReasonMalformedPayload     = "malformed_payload"
)

var (
	ErrNotFound = errors.New("operation: not found")
	ErrLocked   = errors.New("operation: locked by another worker")
)

type FailedError struct {
	Reason string
}

func (e *FailedError) Error() string {
	return fmt.Sprintf("operation failed: %s", e.Reason)
}

type RecoverableError struct {
	Cause error
}

func (e *RecoverableError) Error() string {
	if e.Cause == nil {
		return "operation recoverable"
	}

	return fmt.Sprintf("operation recoverable: %v", e.Cause)
}

func (e *RecoverableError) Unwrap() error { return e.Cause }
