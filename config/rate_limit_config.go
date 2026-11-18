package config

type RateLimitConfig struct {
	// 0 disables the limit.
	UserCreatePerMinute int `env:"USER_CREATE_RATE_LIMIT_PER_MINUTE" env-default:"60"`
}
