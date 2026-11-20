package rabbitmq

import (
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrPublishNacked     = errors.New("rabbitmq: broker did not confirm the message")
	ErrPublishUnroutable = errors.New("rabbitmq: broker confirmed the message but no queue took it")
	ErrDeliveriesStopped = errors.New("rabbitmq: delivery channel closed")
	ErrNotConnected      = errors.New("rabbitmq: not connected")
)

// IsFatal reports whether the broker will keep rejecting the same request until
// someone changes the credentials or the topology. Reconnecting on those codes
// turns a configuration mistake into an endless retry loop that only shows up
// in the logs, so callers are expected to give up instead.
func IsFatal(err error) bool {
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) {
		return false
	}

	switch amqpErr.Code {
	case amqp.AccessRefused, amqp.NotAllowed, amqp.NotFound, amqp.ResourceLocked, amqp.PreconditionFailed, amqp.CommandInvalid:
		return true
	default:
		return false
	}
}

type PermanentError struct {
	Err error
}

func NewPermanentError(err error) *PermanentError {
	return &PermanentError{Err: err}
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("permanent: %v", e.Err)
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

type RetryAfterError struct {
	Err   error
	Delay time.Duration
}

func NewRetryAfterError(err error, delay time.Duration) *RetryAfterError {
	return &RetryAfterError{Err: err, Delay: delay}
}

func (e *RetryAfterError) Error() string {
	return fmt.Sprintf("retry after %s: %v", e.Delay, e.Err)
}

func (e *RetryAfterError) Unwrap() error {
	return e.Err
}
