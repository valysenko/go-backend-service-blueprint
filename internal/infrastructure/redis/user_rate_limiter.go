package redis

import (
	"context"
	"fmt"
	"time"

	appuser "go-backend-service-blueprint/internal/app/user"
	"go-backend-service-blueprint/pkg/redis"

	goredis "github.com/redis/go-redis/v9"
)

const rateLimiterKeyPrefix = "rate_limit:"

var _ appuser.RateLimiter = (*RateLimiter)(nil)

// Fixed window:
//   - the key carries the window start, so the counter resets by expiring rather than by being reset.
//   - the known trade-off is the window seam — a client can spend its full quota at the end of one window and again at the start of the next.
type RateLimiter struct {
	redis *redis.AppRedis
	now   func() time.Time
}

func NewRateLimiter(appRedis *redis.AppRedis) *RateLimiter {
	return &RateLimiter{redis: appRedis, now: time.Now}
}

func (l *RateLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, time.Duration, error) {
	now := l.now()
	windowStart := now.Truncate(window)
	redisKey := fmt.Sprintf("%s%s:%d", rateLimiterKeyPrefix, key, windowStart.Unix())

	var count *goredis.IntCmd
	_, err := l.redis.Client.TxPipelined(ctx, func(pipe goredis.Pipeliner) error {
		count = pipe.Incr(ctx, redisKey)
		pipe.Expire(ctx, redisKey, window)

		return nil
	})
	if err != nil {
		return false, 0, fmt.Errorf("redis: count requests for %q: %w", key, err)
	}

	if count.Val() > int64(limit) {
		return false, windowStart.Add(window).Sub(now), nil
	}

	return true, 0, nil
}
