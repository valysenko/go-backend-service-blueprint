package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type testHandler func(context.Context, Delivery) error

func (f testHandler) Handle(ctx context.Context, d Delivery) error { return f(ctx, d) }

type acknowledger struct{ acked, nacked, requeue bool }

func (a *acknowledger) Ack(_ uint64, _ bool) error { a.acked = true; return nil }
func (a *acknowledger) Nack(_ uint64, _, requeue bool) error {
	a.nacked, a.requeue = true, requeue
	return nil
}
func (a *acknowledger) Reject(_ uint64, requeue bool) error {
	a.nacked, a.requeue = true, requeue
	return nil
}

func TestConsumerSettlesMessages(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		handle               testHandler
		cancel               bool
		wantAck, wantRequeue bool
	}{
		{name: "success", handle: func(context.Context, Delivery) error { return nil }, wantAck: true},
		{name: "permanent failure", handle: func(context.Context, Delivery) error { return NewPermanentError(errors.New("invalid")) }},
		{name: "retries exhausted", handle: func(context.Context, Delivery) error { return errors.New("temporary") }},
		{name: "shutdown requeues", handle: func(context.Context, Delivery) error { return errors.New("interrupted") }, cancel: true, wantRequeue: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewConsumer(Config{}, ConsumerConfig{Spec: QueueSpec{Name: "test"}, Retry: RetryPolicy{MaxAttempts: 2}, Handler: tc.handle}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			a := &acknowledger{}
			c.process(ctx, amqp.Delivery{Acknowledger: a})
			require.Equal(t, tc.wantAck, a.acked)
			require.Equal(t, !tc.wantAck, a.nacked)
			require.Equal(t, tc.wantRequeue, a.requeue)
		})
	}
}

func TestRetryAfterDoesNotSpendAttempt(t *testing.T) {
	attempts := []int{}
	c, err := NewConsumer(Config{}, ConsumerConfig{Spec: QueueSpec{Name: "test"}, Retry: RetryPolicy{MaxAttempts: 2}, Handler: testHandler(func(_ context.Context, d Delivery) error {
		attempts = append(attempts, d.Attempt)
		if len(attempts) == 1 {
			return NewRetryAfterError(errors.New("busy"), time.Millisecond)
		}
		return nil
	})}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	a := &acknowledger{}
	c.process(context.Background(), amqp.Delivery{Acknowledger: a})
	require.Equal(t, []int{1, 1}, attempts)
	require.True(t, a.acked)
}

func TestThrottledRetriesStopAtMaxElapsed(t *testing.T) {
	calls := 0
	c, err := NewConsumer(Config{}, ConsumerConfig{Spec: QueueSpec{Name: "test"}, Retry: RetryPolicy{
		MaxAttempts:    2,
		InitialBackoff: 5 * time.Millisecond,
		MaxElapsed:     50 * time.Millisecond,
	}, Handler: testHandler(func(context.Context, Delivery) error {
		calls++
		return NewRetryAfterError(errors.New("busy"), 5*time.Millisecond)
	})}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	a := &acknowledger{}
	c.process(context.Background(), amqp.Delivery{Acknowledger: a})
	require.Greater(t, calls, 1, "throttling must not spend an attempt")
	require.True(t, a.nacked)
	require.False(t, a.requeue, "an exhausted message belongs in the dead letter queue")
}

func TestStuckHandlerStopsAtMaxElapsed(t *testing.T) {
	c, err := NewConsumer(Config{}, ConsumerConfig{Spec: QueueSpec{Name: "test"}, Retry: RetryPolicy{
		MaxAttempts: 5,
		MaxElapsed:  30 * time.Millisecond,
	}, Handler: testHandler(func(ctx context.Context, _ Delivery) error {
		<-ctx.Done()
		return ctx.Err()
	})}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	a := &acknowledger{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.process(context.Background(), amqp.Delivery{Acknowledger: a})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler was never cut off")
	}
	require.True(t, a.nacked)
	require.False(t, a.requeue, "a message that ran out of budget belongs in the dead letter queue")
}

func TestIsFatal(t *testing.T) {
	require.True(t, IsFatal(fmt.Errorf("declare queue: %w", &amqp.Error{Code: amqp.PreconditionFailed})))
	require.True(t, IsFatal(&amqp.Error{Code: amqp.AccessRefused}))
	require.False(t, IsFatal(amqp.ErrClosed))
	require.False(t, IsFatal(ErrDeliveriesStopped))
	require.False(t, IsFatal(nil))
}

func TestChannelErrorMakesABrokerCloseFatal(t *testing.T) {
	closed := make(chan *amqp.Error, 1)
	require.Nil(t, channelError(closed))

	closed <- &amqp.Error{Code: amqp.PreconditionFailed, Reason: "delivery acknowledgement timed out"}
	reason := channelError(closed)
	require.NotNil(t, reason)
	require.True(t, IsFatal(fmt.Errorf("rabbitmq: consume %q: %w", "test", reason)))
}

func TestQueueValidation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := testHandler(func(context.Context, Delivery) error { return nil })
	reg := ConsumerConfig{Spec: QueueSpec{Name: "q"}, Handler: h}
	_, err := NewConsumerSet(Config{}, []ConsumerConfig{reg, reg}, nil, logger)
	require.Error(t, err)
	_, err = NewConsumer(Config{}, ConsumerConfig{Spec: QueueSpec{Name: "q", Kind: "unknown"}, Handler: h}, nil, logger)
	require.Error(t, err)
	err = NewPublisher(Config{}, QueueSpec{Name: "q", Kind: KindDirect}).PublishWithDelay(context.Background(), Message{}, time.Second)
	require.Error(t, err)
}
