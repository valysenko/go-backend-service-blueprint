package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go-backend-service-blueprint/internal/testutils"
	"go-backend-service-blueprint/pkg/ctxutils"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

// This starts both real binaries; the schema registry HTTP endpoint serves the
// committed schema while Kafka and Redis run in isolated containers.
func TestKafkaAppsEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker")
	}
	ctx := context.Background()
	redisURL, stopRedis, err := testutils.RunRedisContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(stopRedis)
	brokers, stopKafka, err := testutils.RunKafkaContainer(ctx,
		"example_user_topic", "example_user_topic.dl", "example_events_topic", "example_events_topic.dl")
	require.NoError(t, err)
	t.Cleanup(stopKafka)

	schema, err := os.ReadFile("../../deployments/kafka/schemas/example_user_topic-value.json")
	require.NoError(t, err)
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects/example_user_topic-value/versions/1" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"subject": "example_user_topic-value", "version": 1, "id": 1, "schemaType": "JSON", "schema": string(schema)})
	}))
	t.Cleanup(registry.Close)

	userPort := freePort(t)
	eventsPort := freePort(t)
	startKafkaBinary(t, "../kafka-user", userPort, redisURL, brokers[0], registry.URL)
	startKafkaBinary(t, "../kafka-events", eventsPort, redisURL, brokers[0], registry.URL)
	waitForKafkaHTTP(t, userPort)
	waitForKafkaHTTP(t, eventsPort)

	key := uuid.NewString()
	payload := fmt.Sprintf(`{"uuid":%q,"email":"e2e@example.com"}`, uuid.NewString())
	testutils.WriteKafkaMessage(t, brokers, "example_user_topic", []byte(key), []byte(payload), map[string]string{ctxutils.RequestID: "e2e-request"})
	messages := testutils.ReadNKafkaMessages(t, brokers, "example_events_topic", 0, 1, 30*time.Second)
	require.Equal(t, key, string(messages[0].Key))
	var event struct {
		EventID   uuid.UUID `json:"eventId"`
		SomeField string    `json:"someField"`
	}
	require.NoError(t, json.Unmarshal(messages[0].Value, &event))
	require.NotEqual(t, uuid.Nil, event.EventID)
	require.Equal(t, "e2e@example.com", event.SomeField)
	require.Contains(t, messages[0].Headers, kafkago.Header{Key: ctxutils.RequestID, Value: []byte("e2e-request")})

	// The second app has to process the derived event, not just receive it on Kafka.
	require.Eventually(t, func() bool {
		response, err := http.Get("http://127.0.0.1:" + eventsPort + "/metrics")
		if err != nil {
			return false
		}
		defer func() { _ = response.Body.Close() }()
		body, err := io.ReadAll(response.Body)
		return err == nil && strings.Contains(string(body), `kafka_consumer_events_total{status="success",topic="example_events_topic"} 1`)
	}, 30*time.Second, 200*time.Millisecond)

	badKey := uuid.NewString()
	testutils.WriteKafkaMessage(t, brokers, "example_user_topic", []byte(badKey), []byte(`{"uuid":"invalid","email":"bad"}`), nil)
	dl := testutils.ReadNKafkaMessages(t, brokers, "example_user_topic.dl", 0, 1, 30*time.Second)
	require.Equal(t, badKey, string(dl[0].Key))
}

func freePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return fmt.Sprint(port)
}

func startKafkaBinary(t *testing.T, path, port, redisURL, broker, registry string) {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "consumer")
	build := exec.Command("go", "build", "-o", binary, path)
	output, err := build.CombinedOutput()
	require.NoError(t, err, "build %s: %s", path, output)
	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(), "REDIS_URL="+redisURL, "REDIS_PASSWORD=", "REDIS_DB=0", "KAFKA_BROKERS="+broker, "KAFKA_MONITORING_PORT="+port, "SCHEMA_REGISTRY_URL="+registry)
	logFile, err := os.Create(filepath.Join(t.TempDir(), "consumer.log"))
	require.NoError(t, err)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("%s exited: %v", path, err)
			}
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Errorf("%s did not shut down", path)
		}
		_ = logFile.Close()
		if t.Failed() {
			output, _ := os.ReadFile(logFile.Name())
			t.Logf("%s logs: %s", path, output)
		}
	})
}

func waitForKafkaHTTP(t *testing.T, port string) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	require.Eventually(t, func() bool {
		response, err := client.Get("http://127.0.0.1:" + port + "/healthcheck")
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 30*time.Second, 200*time.Millisecond)
}
