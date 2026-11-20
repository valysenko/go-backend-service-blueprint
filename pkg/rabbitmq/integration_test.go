package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-backend-service-blueprint/internal/testutils"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

var integrationBroker testutils.RabbitMQContainer
var stopIntegrationBroker func()
var integrationOnce sync.Once
var integrationErr error

func TestMain(m *testing.M) {
	code := m.Run()
	if stopIntegrationBroker != nil {
		stopIntegrationBroker()
	}
	os.Exit(code)
}

func integrationConfig(t *testing.T) Config {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test needs Docker")
	}
	integrationOnce.Do(func() {
		integrationBroker, stopIntegrationBroker, integrationErr = testutils.RunRabbitMQContainer(context.Background())
	})
	require.NoError(t, integrationErr)
	return Config{Host: integrationBroker.Host, Port: integrationBroker.Port, User: integrationBroker.User, Password: integrationBroker.Password}
}

func deleteTopology(t *testing.T, cfg Config, spec QueueSpec) {
	t.Helper()

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
				deleteTopology(t, cfg, spec)
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

func TestBrokerNackIsNotSuccess(t *testing.T) {
	cfg := integrationConfig(t)
	baseURL := managementURL(t)
	spec := QueueSpec{Name: "test." + uuid.NewString(), Kind: KindDirect}
	pub := NewPublisher(cfg, spec)
	require.NoError(t, pub.Connect())
	t.Cleanup(func() {
		require.NoError(t, pub.Close())
		deleteTopology(t, cfg, spec)
	})

	ctx := context.Background()
	require.NoError(t, pub.Publish(ctx, Message{Body: []byte("accepted")}))

	// A queue that may hold nothing and rejects the overflow is the only way to
	// make the broker nack a well formed publish. It has to come from a policy:
	// queue arguments would have to match the ones Declare already uses.
	policy := "reject-" + spec.Name
	setPolicy(t, cfg, baseURL, policy, spec.Name)
	t.Cleanup(func() { callManagement(t, cfg, http.MethodDelete, baseURL+"/api/policies/%2F/"+policy, "") })

	// Policies reach a queue asynchronously, so the nack is not the first answer.
	require.Eventually(t, func() bool {
		return errors.Is(pub.Publish(ctx, Message{Body: []byte("rejected")}), ErrPublishNacked)
	}, 10*time.Second, 200*time.Millisecond, "the broker never nacked the publish")
}

func managementURL(t *testing.T) string {
	t.Helper()
	return "http://" + net.JoinHostPort(integrationBroker.Host, integrationBroker.ManagementPort)
}

func setPolicy(t *testing.T, cfg Config, baseURL, name, queue string) {
	t.Helper()

	body := fmt.Sprintf(
		`{"pattern":%q,"apply-to":"queues","priority":1,"definition":{"max-length":0,"overflow":"reject-publish"}}`,
		"^"+regexp.QuoteMeta(queue)+"$",
	)
	callManagement(t, cfg, http.MethodPut, baseURL+"/api/policies/%2F/"+name, body)
}

func callManagement(t *testing.T, cfg Config, method, url, body string) {
	t.Helper()

	request, err := http.NewRequest(method, url, strings.NewReader(body))
	require.NoError(t, err)
	request.SetBasicAuth(cfg.User, cfg.Password)
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()
	require.Less(t, response.StatusCode, http.StatusMultipleChoices, "%s %s", method, url)
}

func TestBrokerReconnectsAfterConnectionDrop(t *testing.T) {
	cfg := integrationConfig(t)
	spec := QueueSpec{Name: "test." + uuid.NewString()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	deliveries := make(chan Delivery, 1)
	consumer, err := NewConsumer(cfg, ConsumerConfig{Spec: spec, Handler: testHandler(func(_ context.Context, d Delivery) error {
		deliveries <- d
		return nil
	})}, nil, logger)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
		deleteTopology(t, cfg, spec)
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

	// A fresh connection proves nothing on its own: a consumer that reconnects
	// but never resumes consuming looks exactly the same from the outside.
	pub := NewPublisher(cfg, spec)
	t.Cleanup(func() { require.NoError(t, pub.Close()) })
	require.NoError(t, pub.Publish(context.Background(), Message{Body: []byte("after reconnect")}))
	select {
	case d := <-deliveries:
		require.Equal(t, "after reconnect", string(d.Body))
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer reconnected but stopped delivering messages")
	}
}

func TestDelayedReplayReturnsUnroutableMessageToDLQ(t *testing.T) {
	cfg := integrationConfig(t)
	spec := QueueSpec{Name: "test." + uuid.NewString(), Kind: KindDelayed}
	conn, err := Dial(cfg)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	require.NoError(t, Declare(ch, spec))
	t.Cleanup(func() { deleteTopology(t, cfg, spec) })
	require.NoError(t, ch.Confirm(false))
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	// Put a message in the DLQ, then remove its destination after declaration.
	err = ch.PublishWithContext(context.Background(), "", DeadLetterQueueName(spec.Name), false, false, amqp.Publishing{Body: []byte("replay")})
	require.NoError(t, err)
	delivery, ok, err := ch.Get(DeadLetterQueueName(spec.Name), false)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = ch.QueueDelete(spec.Name, false, false, false)
	require.NoError(t, err)
	err = republish(context.Background(), ch, returns, spec, delivery)
	require.ErrorIs(t, err, ErrPublishUnroutable)
	require.NoError(t, delivery.Nack(false, true))
	require.Eventually(t, func() bool {
		queue, err := ch.QueueDeclarePassive(DeadLetterQueueName(spec.Name), true, false, false, false, nil)
		return err == nil && queue.Messages == 1
	}, 5*time.Second, 20*time.Millisecond)
}
