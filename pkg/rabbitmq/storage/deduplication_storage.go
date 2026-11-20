package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	appredis "go-backend-service-blueprint/pkg/redis"
)

const rabbitRetention = 24 * time.Hour

// DeduplicationStorage records completed messages by queue and message ID in Redis.
type DeduplicationStorage struct {
	redis *appredis.AppRedis
}

func NewDeduplicationStorage(redis *appredis.AppRedis) *DeduplicationStorage {
	return &DeduplicationStorage{redis: redis}
}

func (s *DeduplicationStorage) IsProcessed(ctx context.Context, queue, messageID string) (bool, error) {
	count, err := s.redis.Client.Exists(ctx, deduplicationKey(queue, messageID)).Result()
	if err != nil {
		return false, fmt.Errorf("check rabbitmq message completion: %w", err)
	}
	return count > 0, nil
}

func (s *DeduplicationStorage) MarkProcessed(ctx context.Context, queue, messageID string) error {
	if err := s.redis.Client.Set(ctx, deduplicationKey(queue, messageID), "done", rabbitRetention).Err(); err != nil {
		return fmt.Errorf("record rabbitmq message completion: %w", err)
	}
	return nil
}

func deduplicationKey(queue, messageID string) string {
	sum := sha256.Sum256([]byte(queue + "\x00" + messageID))
	return "rabbitmq:dedup:" + hex.EncodeToString(sum[:])
}
