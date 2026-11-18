package config

import (
	"log"

	"github.com/ilyakaznacheev/cleanenv"
)

type CronAppConfig struct {
	AppEnv         string `env:"APP_ENV" env-default:"loc"`
	PodName        string `env:"POD_NAME" env-default:"pod_name_was_not_set"`
	Namespace      string `env:"NAMESPACE" env-default:"blueprint"`
	PushgatewayURL string `env:"PUSHGATEWAY_URL" env-default:""`
	BatchSize      int    `env:"CRON_BATCH_SIZE" env-default:"100"`
	PostgreSQLConfig
}

func InitCronAppConfig() *CronAppConfig {
	cfg := &CronAppConfig{}

	if err := cleanenv.ReadEnv(cfg); err != nil {
		log.Fatalf("Error reading environment variables: %v", err)
	}

	return cfg
}
