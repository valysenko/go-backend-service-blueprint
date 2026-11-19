package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/redis"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const deduplicationCacheValue = true
const deduplicationCacheKeyTTL = 24 * time.Hour
const idHeaderKey = "id"

type DeduplicationStorage struct {
	redis         *redis.AppRedis
	consumerGroup string
}

func NewDeduplicationStorage(redis *redis.AppRedis, cg string) *DeduplicationStorage {
	return &DeduplicationStorage{
		redis:         redis,
		consumerGroup: cg,
	}
}

func (h *DeduplicationStorage) IsAcked(ctx context.Context, msg *reader.Message) (bool, error) {
	key, err := h.getCacheKey(msg)
	if err != nil {
		return false, err
	}

	count, err := h.redis.Client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

func (h *DeduplicationStorage) Ack(ctx context.Context, msg *reader.Message) error {
	key, err := h.getCacheKey(msg)
	if err != nil {
		return err
	}

	_, err = h.redis.Client.SetArgs(ctx, key, deduplicationCacheValue, goredis.SetArgs{
		Mode: "NX",
		TTL:  deduplicationCacheKeyTTL,
	}).Result()
	if errors.Is(err, goredis.Nil) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("set ack key in redis: %w", err)
	}
	return nil
}

func (h *DeduplicationStorage) getCacheKey(msg *reader.Message) (string, error) {
	topic := msg.Topic()
	id, err := msg.GetHeader(idHeaderKey)
	if err != nil {
		return fmt.Sprintf("dedup_key_%s_%s_content_%s", topic, h.consumerGroup, contentHash(msg)), nil
	}

	return fmt.Sprintf("dedup_key_%s_%s_%s", topic, h.consumerGroup, id), nil
}

func contentHash(msg *reader.Message) string {
	hash := sha256.New()
	hash.Write(msg.Key())
	hash.Write([]byte{0})
	hash.Write(msg.Body())
	return hex.EncodeToString(hash.Sum(nil))
}
