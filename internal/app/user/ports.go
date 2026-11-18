package user

import (
	"context"
	"time"
)

// RateLimiter reports whether key may spend one more slot of its window, and
// how long the caller should wait when it may not.
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, time.Duration, error)
}

type unlimited struct{}

func NewUnlimited() RateLimiter {
	return unlimited{}
}

func (unlimited) Allow(context.Context, string, int, time.Duration) (bool, time.Duration, error) {
	return true, 0, nil
}
