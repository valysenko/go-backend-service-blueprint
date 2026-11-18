package user

import (
	"fmt"
	"time"
)

type RateLimitedError struct {
	RetryAfter time.Duration
}

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("user: rate limited, retry after %s", e.RetryAfter)
}
