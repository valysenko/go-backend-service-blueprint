package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"go-backend-service-blueprint/cmd/shared"
	"go-backend-service-blueprint/config"
	"go-backend-service-blueprint/internal/testutils"
	"go-backend-service-blueprint/pkg/ctxutils"
	"go-backend-service-blueprint/pkg/rabbitmq"
	"go-backend-service-blueprint/pkg/rabbitmq/storage"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestRabbitMQAppEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker")
	}
	ctx := ctxutils.WithRequestID(context.Background(), "rabbit-e2e")
	container, stop, err := testutils.RunRabbitMQContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(stop)
	redisURL, stopRedis, err := testutils.RunRedisContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(stopRedis)
	redis, err := shared.NewRedis(config.RedisConfig{RedisURL: redisURL})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, redis.Close()) })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	monitoringPort := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	cfg := &config.RabbitMQAppConfig{Host: container.Host, Port: container.Port, User: container.User, Password: container.Password, MonitoringPort: strconv.Itoa(monitoringPort)}
	app, err := NewRabbitMQApp(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewDeduplicationStorage(redis), redis)
	require.NoError(t, err)
	appCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- app.Run(appCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Error("rabbitmq app did not shut down")
		}
	})
	client := &http.Client{Timeout: time.Second}
	require.Eventually(t, func() bool {
		response, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/healthcheck", monitoringPort))
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 20*time.Second, 100*time.Millisecond)

	broker := connectionConfig(cfg)
	conn, err := rabbitmq.Dial(broker)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	// Declare before starting reads: the application publisher creates it lazily.
	require.NoError(t, rabbitmq.Declare(ch, outputQueue))
	for _, spec := range queueSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			publisher := rabbitmq.NewPublisher(broker, spec)
			defer func() { require.NoError(t, publisher.Close()) }()
			msg := rabbitmq.Message{ID: uuid.NewString(), Body: []byte(`{"text":"e2e"}`)}
			start := time.Now()
			if spec.Kind == rabbitmq.KindDelayed {
				err = publisher.PublishWithDelay(ctx, msg, 600*time.Millisecond)
			} else {
				err = publisher.Publish(ctx, msg)
			}
			require.NoError(t, err)
			var message amqp.Delivery
			var readErr error
			require.Eventually(t, func() bool {
				var ok bool
				message, ok, readErr = ch.Get(outputQueue.Name, false)
				return readErr != nil || ok
			}, 15*time.Second, 50*time.Millisecond)
			require.NoError(t, readErr, "read output")
			require.NoError(t, message.Ack(false))
			var output struct {
				SourceQueue string `json:"sourceQueue"`
				Text        string `json:"text"`
			}
			require.NoError(t, json.Unmarshal(message.Body, &output))
			require.Equal(t, spec.Name, output.SourceQueue)
			require.Equal(t, "e2e", output.Text)
			require.Equal(t, "rabbit-e2e", message.Headers[ctxutils.RequestID])
			require.NotEmpty(t, message.MessageId)

			if spec.Kind == rabbitmq.KindDelayed {
				require.GreaterOrEqual(t, time.Since(start), 500*time.Millisecond)
			}
			// A second delivery with the same message ID must not emit a second
			// result after the first delivery was recorded as complete.
			require.NoError(t, publisher.Publish(ctx, msg))
			require.Eventually(t, func() bool {
				response, requestErr := client.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", monitoringPort))
				if requestErr != nil {
					return false
				}
				defer func() { _ = response.Body.Close() }()
				body, readErr := io.ReadAll(response.Body)
				metric := fmt.Sprintf(`rabbitmq_consumer_messages_total{outcome="processed",queue=%q} 2`, spec.Name)
				return readErr == nil && strings.Contains(string(body), metric)
			}, 10*time.Second, 50*time.Millisecond)
			_, duplicate, err := ch.Get(outputQueue.Name, true)
			require.NoError(t, err)
			require.False(t, duplicate, "duplicate input produced a second output")
			require.NoError(t, publisher.Publish(ctx, rabbitmq.Message{ID: uuid.NewString(), Body: []byte(`{"text":""}`)}))
			require.Eventually(t, func() bool {
				queue, inspectErr := ch.QueueDeclarePassive(rabbitmq.DeadLetterQueueName(spec.Name), true, false, false, false, nil)
				return inspectErr == nil && queue.Messages == 1
			}, 10*time.Second, 50*time.Millisecond)
		})
	}
}
