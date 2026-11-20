package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Set RABBITMQ_INTEGRATION_PORT to run against a broker with the delayed plugin.
func integrationConfig(t *testing.T) Config {
	t.Helper()

	portText := os.Getenv("RABBITMQ_INTEGRATION_PORT")
	if portText == "" || testing.Short() {
		t.Skip("set RABBITMQ_INTEGRATION_PORT to test against a RabbitMQ broker")
	}
	port, err := strconv.Atoi(portText)
	require.NoError(t, err)

	return Config{Host: "localhost", Port: port, User: "blueprint", Password: "blueprint"}
}

func TestBrokerDeliveryAndDeadLetters(t *testing.T) {
	cfg := integrationConfig(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, kind := range []Kind{KindDirect, KindDelayed} {
		t.Run(string(kind), func(t *testing.T) {
			spec := QueueSpec{Name: "test." + uuid.NewString(), Kind: kind}
			pub := NewPublisher(cfg, spec)
			require.NoError(t, pub.Connect())
			t.Cleanup(func() {
				require.NoError(t, pub.Close())
				conn, err := Dial(cfg)
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = conn.Close() }()
				ch, err := conn.Channel()
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = ch.Close() }()
				_, _ = ch.QueueDelete(spec.Name, false, false, false)
				_, _ = ch.QueueDelete(DeadLetterQueueName(spec.Name), false, false, false)
				if spec.Kind == KindDelayed {
					_ = ch.ExchangeDelete(spec.exchange(), false, false)
				}
			})
			deliveries := make(chan Delivery, 2)
			var allowBad atomic.Bool
			consumer, err := NewConsumer(cfg, ConsumerConfig{Spec: spec, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond}, Handler: testHandler(func(_ context.Context, d Delivery) error {
				if string(d.Body) == "bad" && !allowBad.Load() {
					return NewPermanentError(errors.New("invalid"))
				}
				deliveries <- d
				return nil
			})}, nil, logger)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- consumer.Run(ctx) }()
			t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
			require.Eventually(t, func() bool { return consumer.Ping() == nil }, 5*time.Second, 20*time.Millisecond)
			start := time.Now()
			if kind == KindDelayed {
				require.NoError(t, pub.PublishWithDelay(ctx, Message{Body: []byte("ok")}, 600*time.Millisecond))
			} else {
				require.NoError(t, pub.Publish(ctx, Message{Body: []byte("ok")}))
			}
			select {
			case <-deliveries:
				if kind == KindDelayed {
					require.GreaterOrEqual(t, time.Since(start), 500*time.Millisecond)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("message was not delivered")
			}
			require.NoError(t, pub.Publish(ctx, Message{Body: []byte("bad")}))
			conn, err := Dial(cfg)
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()
			ch, err := conn.Channel()
			require.NoError(t, err)
			defer func() { _ = ch.Close() }()
			require.Eventually(t, func() bool {
				q, err := ch.QueueDeclarePassive(DeadLetterQueueName(spec.Name), true, false, false, false, nil)
				return err == nil && q.Messages == 1
			}, 5*time.Second, 50*time.Millisecond, fmt.Sprintf("message did not reach %s", DeadLetterQueueName(spec.Name)))
			allowBad.Store(true)
			n, err := ReplayDeadLetters(ctx, cfg, spec, 0)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			select {
			case message := <-deliveries:
				require.Equal(t, "bad", string(message.Body))
			case <-time.After(5 * time.Second):
				t.Fatal("replayed message was not delivered")
			}
		})
	}
}

func TestBrokerRejectsAPublishNoQueueTakes(t *testing.T) {
	cfg := integrationConfig(t)
	spec := QueueSpec{Name: "test." + uuid.NewString(), Kind: KindDirect}
	pub := NewPublisher(cfg, spec)
	require.NoError(t, pub.Connect())
	t.Cleanup(func() { require.NoError(t, pub.Close()) })

	ctx := context.Background()
	require.NoError(t, pub.Publish(ctx, Message{Body: []byte("routed")}))

	// Deleting the queue leaves the publisher's channel open and healthy, so a
	// basic.return is the only thing that can tell it the next message is lost.
	conn, err := Dial(cfg)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	_, err = ch.QueueDelete(spec.Name, false, false, false)
	require.NoError(t, err)
	_, err = ch.QueueDelete(DeadLetterQueueName(spec.Name), false, false, false)
	require.NoError(t, err)

	require.ErrorIs(t, pub.Publish(ctx, Message{Body: []byte("lost")}), ErrPublishUnroutable)
}

func TestBrokerReconnectsAfterConnectionDrop(t *testing.T) {
	cfg := integrationConfig(t)
	spec := QueueSpec{Name: "test." + uuid.NewString()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	consumer, err := NewConsumer(cfg, ConsumerConfig{Spec: spec, Handler: testHandler(func(context.Context, Delivery) error { return nil })}, nil, logger)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
		conn, err := Dial(cfg)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		ch, err := conn.Channel()
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = ch.Close() }()
		_, _ = ch.QueueDelete(spec.Name, false, false, false)
		_, _ = ch.QueueDelete(DeadLetterQueueName(spec.Name), false, false, false)
	})
	require.Eventually(t, func() bool { return consumer.Ping() == nil }, 5*time.Second, 20*time.Millisecond)
	consumer.mu.Lock()
	original := consumer.conn
	consumer.mu.Unlock()
	require.NoError(t, original.Close())
	require.Eventually(t, func() bool {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return consumer.conn != nil && consumer.conn != original && !consumer.conn.IsClosed()
	}, 6*time.Second, 50*time.Millisecond)
}
