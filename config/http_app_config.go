package config

import (
	"log"

	"github.com/ilyakaznacheev/cleanenv"
)

type HTTPAppConfig struct {
	AppEnv    string `env:"APP_ENV" env-default:"loc"`
	PodName   string `env:"POD_NAME" env-default:"pod_name_was_not_set"`
	Namespace string `env:"NAMESPACE" env-default:"blueprint"`
	ServerConfig
	PostgreSQLConfig
	RedisConfig
	RateLimitConfig
}

func InitHTTPAppConfig() *HTTPAppConfig {
	cfg := &HTTPAppConfig{}

	if err := cleanenv.ReadEnv(cfg); err != nil {
		log.Fatalf("Error reading environment variables: %v", err)
	}

	return cfg
}
