package testutils

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	redisImage        = "redis:8.4-alpine"
	kafkaImage        = "confluentinc/confluent-local:7.5.0"
	kafkaReadyTimeout = 60 * time.Second
	kafkaDialTimeout  = 5 * time.Second
	kafkaRetryDelay   = 200 * time.Millisecond
)

func RunRedisContainer(ctx context.Context) (string, func(), error) {
	container, err := tcredis.Run(ctx, redisImage)
	if err != nil {
		return "", nil, fmt.Errorf("start redis container: %w", err)
	}
	terminate := func() { _ = testcontainers.TerminateContainer(container) }

	host, err := container.Host(ctx)
	if err != nil {
		terminate()
		return "", nil, fmt.Errorf("redis host: %w", err)
	}
	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		terminate()
		return "", nil, fmt.Errorf("redis port: %w", err)
	}

	return host + ":" + port.Port(), terminate, nil
}

func RunKafkaContainer(ctx context.Context, topics ...string) ([]string, func(), error) {
	container, err := tckafka.Run(ctx, kafkaImage)
	if err != nil {
		return nil, nil, fmt.Errorf("start kafka container: %w", err)
	}
	terminate := func() { _ = testcontainers.TerminateContainer(container) }

	brokers, err := container.Brokers(ctx)
	if err != nil {
		terminate()
		return nil, nil, fmt.Errorf("kafka brokers: %w", err)
	}

	if len(topics) > 0 {
		if err := createKafkaTopics(brokers[0], topics); err != nil {
			terminate()
			return nil, nil, err
		}
	}

	return brokers, terminate, nil
}

func WriteKafkaMessage(t *testing.T, brokers []string, topic string, key, value []byte, headers map[string]string) {
	t.Helper()

	writer := &kafkago.Writer{
		Addr:     kafkago.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	}
	defer func() { _ = writer.Close() }()

	msg := kafkago.Message{Key: key, Value: value}
	for k, v := range headers {
		msg.Headers = append(msg.Headers, kafkago.Header{Key: k, Value: []byte(v)})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := writer.WriteMessages(ctx, msg); err != nil {
		t.Fatalf("write kafka message: %v", err)
	}
}

func KafkaNextOffset(t *testing.T, brokers []string, topic string) int64 {
	t.Helper()

	if len(brokers) == 0 {
		t.Fatal("KafkaNextOffset: no kafka brokers")
	}

	conn, err := kafkago.DialLeader(context.Background(), "tcp", brokers[0], topic, 0)
	if err != nil {
		t.Fatalf("dial kafka leader: %v", err)
	}
	defer func() { _ = conn.Close() }()

	offset, err := conn.ReadLastOffset()
	if err != nil {
		t.Fatalf("read last offset: %v", err)
	}
	return offset
}

func ReadNKafkaMessages(t *testing.T, brokers []string, topic string, fromOffset int64, n int, timeout time.Duration) []kafkago.Message {
	t.Helper()
	reader := newOffsetReader(t, brokers, topic, fromOffset)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	msgs := make([]kafkago.Message, 0, n)
	for len(msgs) < n {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read kafka message (%d/%d) on %q: %v", len(msgs), n, topic, err)
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

func ReadKafkaMessages(t *testing.T, brokers []string, topic string, fromOffset int64, idle time.Duration) []kafkago.Message {
	t.Helper()

	reader := newOffsetReader(t, brokers, topic, fromOffset)

	var msgs []kafkago.Message
	for {
		ctx, cancel := context.WithTimeout(context.Background(), idle)
		msg, err := reader.ReadMessage(ctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return msgs
			}
			t.Fatalf("read kafka message: %v", err)
		}
		msgs = append(msgs, msg)
	}
}

func newOffsetReader(t *testing.T, brokers []string, topic string, fromOffset int64) *kafkago.Reader {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MaxWait:   250 * time.Millisecond,
	})
	if err := reader.SetOffset(fromOffset); err != nil {
		t.Fatalf("set kafka offset: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	return reader
}

func createKafkaTopics(broker string, topics []string) error {
	configs := make([]kafkago.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafkago.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}

	deadline := time.Now().Add(kafkaReadyTimeout)
	for {
		err := createTopics(broker, configs)
		if err == nil {
			return waitForTopicLeaders(broker, topics, 30*time.Second)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("create kafka topics after %s: %w", kafkaReadyTimeout, err)
		}
		time.Sleep(kafkaRetryDelay)
	}
}

func createTopics(broker string, configs []kafkago.TopicConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), kafkaDialTimeout)
	defer cancel()

	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer func() { _ = conn.Close() }()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("kafka controller: %w", err)
	}

	controllerConn, err := kafkago.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial kafka controller: %w", err)
	}
	defer func() { _ = controllerConn.Close() }()

	if err := controllerConn.CreateTopics(configs...); err != nil && !errors.Is(err, kafkago.TopicAlreadyExists) {
		return fmt.Errorf("create kafka topics: %w", err)
	}

	return nil
}

func waitForTopicLeaders(broker string, topics []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for _, topic := range topics {
		for {
			err := probeTopicLeader(broker, topic)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("topic %q has no leader after %s: %w", topic, timeout, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	return nil
}

func probeTopicLeader(broker, topic string) (retErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := kafkago.DialLeader(ctx, "tcp", broker, topic, 0)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil && retErr == nil {
			retErr = closeErr
		}
	}()

	_, err = conn.ReadLastOffset()
	return err
}

type PostgresContainer struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
}

func RunPostgresContainer(ctx context.Context) (PostgresContainer, func(), error) {
	const user, password, dbName = "blueprint", "blueprint", "blueprint_test"
	container, err := tcpostgres.Run(ctx, "postgres:17.2-alpine",
		tcpostgres.WithDatabase(dbName), tcpostgres.WithUsername(user),
		tcpostgres.WithPassword(password), tcpostgres.BasicWaitStrategies())
	if err != nil {
		return PostgresContainer{}, nil, fmt.Errorf("start postgres container: %w", err)
	}
	stop := func() { _ = testcontainers.TerminateContainer(container) }
	host, err := container.Host(ctx)
	if err != nil {
		stop()
		return PostgresContainer{}, nil, err
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		stop()
		return PostgresContainer{}, nil, err
	}
	return PostgresContainer{Host: host, Port: port.Port(), User: user, Password: password, DBName: dbName}, stop, nil
}

type RabbitMQContainer struct {
	Host           string
	Port           int
	ManagementPort string
	User           string
	Password       string
}

// RunRabbitMQContainer uses the project's broker image and delayed exchange plugin.
func RunRabbitMQContainer(ctx context.Context) (RabbitMQContainer, func(), error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return RabbitMQContainer{}, nil, errors.New("locate rabbitmq Dockerfile")
	}
	dockerContext := filepath.Join(filepath.Dir(file), "../../deployments/rabbitmq")
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{Context: dockerContext, Dockerfile: "Dockerfile"},
			Env:            map[string]string{"RABBITMQ_DEFAULT_USER": "blueprint", "RABBITMQ_DEFAULT_PASS": "blueprint"},
			ExposedPorts:   []string{"5672/tcp", "15672/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForLog("Server startup complete"),
				wait.ForHTTP("/api/overview").WithPort("15672/tcp").WithBasicAuth("blueprint", "blueprint"),
			).WithDeadline(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return RabbitMQContainer{}, nil, fmt.Errorf("start rabbitmq container: %w", err)
	}
	stop := func() { _ = testcontainers.TerminateContainer(container) }
	host, err := container.Host(ctx)
	if err != nil {
		stop()
		return RabbitMQContainer{}, nil, err
	}
	mapped, err := container.MappedPort(ctx, "5672/tcp")
	if err != nil {
		stop()
		return RabbitMQContainer{}, nil, err
	}
	port, err := strconv.Atoi(mapped.Port())
	if err != nil {
		stop()
		return RabbitMQContainer{}, nil, err
	}
	management, err := container.MappedPort(ctx, "15672/tcp")
	if err != nil {
		stop()
		return RabbitMQContainer{}, nil, err
	}
	return RabbitMQContainer{Host: host, Port: port, ManagementPort: management.Port(), User: "blueprint", Password: "blueprint"}, stop, nil
}
