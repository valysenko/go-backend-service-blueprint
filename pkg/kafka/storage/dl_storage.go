package storage

import (
	"context"
	"errors"
	"fmt"
	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/redis"
	"strconv"

	goredis "github.com/redis/go-redis/v9"
)

type DLSource struct {
	Topic         string
	ConsumerGroup string
}

type DlStorage struct {
	redis  *redis.AppRedis
	source DLSource
}

func NewDlStorage(redis *redis.AppRedis, source DLSource) *DlStorage {
	return &DlStorage{
		redis:  redis,
		source: source,
	}
}

func (h *DlStorage) GetDlCounter(ctx context.Context, msg *reader.Message) (int, error) {
	cacheKey := h.getDlCounterKey(msg)

	raw, err := h.redis.Client.Get(ctx, cacheKey).Result()
	if errors.Is(err, goredis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get dl counter from redis: %w", err)
	}

	count, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid dl counter value %q: %w", raw, err)
	}
	return count, nil
}

func (h *DlStorage) IncrementCounter(ctx context.Context, msg *reader.Message) error {
	cacheKey := h.getDlCounterKey(msg)

	err := h.redis.Client.Incr(ctx, cacheKey).Err()
	if err != nil {
		return fmt.Errorf("increment dl counter in redis: %w", err)
	}

	return nil
}

// executes lua script which gets key value and decrements it only if it is >0
func (h *DlStorage) DecrementCounter(ctx context.Context, msg *reader.Message) error {
	cacheKey := h.getDlCounterKey(msg)

	const script = `
local v = tonumber(redis.call('GET', KEYS[1]) or '0')
if v > 0 then return redis.call('DECR', KEYS[1]) end
return v
`
	if err := h.redis.Client.Eval(ctx, script, []string{cacheKey}).Err(); err != nil {
		return fmt.Errorf("decrement dl counter in redis: %w", err)
	}

	return nil
}

// base topic + partition key + base consumer group
func (h *DlStorage) getDlCounterKey(msg *reader.Message) string {
	pk := msg.Key()
	return fmt.Sprintf("dl_counter_%s_%s_%s", h.source.Topic, h.source.ConsumerGroup, pk)
}
