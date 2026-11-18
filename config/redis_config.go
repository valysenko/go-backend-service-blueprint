package config

type RedisConfig struct {
	RedisURL      string `env:"REDIS_URL"`
	RedisPassword string `env:"REDIS_PASSWORD" env-default:""`
	RedisDB       int    `env:"REDIS_DB" env-default:"0"`
}
