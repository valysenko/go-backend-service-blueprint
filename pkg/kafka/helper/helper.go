package helper

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	kafkaPkg "github.com/segmentio/kafka-go"
)

func TopicExists(server, topic string) (exists bool, retErr error) {
	conn, err := kafkaPkg.Dial("tcp", server)
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("closing kafka connection: %w", closeErr))
		}
	}()

	if err = conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return false, fmt.Errorf("setting connection deadline: %w", err)
	}

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return false, err
	}

	for _, p := range partitions {
		if p.Topic == topic {
			return true, nil
		}
	}

	return false, nil
}

func CheckTopicExists(brokers []string, topic string, logger *slog.Logger) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no kafka brokers configured")
	}

	var lastErr error
	for _, broker := range brokers {
		exists, err := TopicExists(broker, topic)
		if err != nil {
			logger.Error("failed to check if kafka topic exists", "broker", broker, "error", err)
			lastErr = err
			continue
		}
		if !exists {
			return fmt.Errorf("topic %q does not exist", topic)
		}
		return nil
	}

	return fmt.Errorf("verify topic %q (all brokers unreachable): %w", topic, errors.Join(lastErr))
}
