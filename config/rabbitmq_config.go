package config

import "github.com/ilyakaznacheev/cleanenv"

type RabbitMQAppConfig struct {
	RedisConfig
	Host           string `env:"RABBITMQ_HOST" env-default:"blueprint-rabbitmq"`
	Port           int    `env:"RABBITMQ_PORT" env-default:"5672"`
	User           string `env:"RABBITMQ_USER" env-default:"blueprint"`
	Password       string `env:"RABBITMQ_PASSWORD" env-default:"blueprint"`
	VHost          string `env:"RABBITMQ_VHOST" env-default:"/"`
	MonitoringPort string `env:"RABBITMQ_MONITORING_PORT" env-default:"8099"`
}

func LoadRabbitMQAppConfig() (*RabbitMQAppConfig, error) {
	cfg := &RabbitMQAppConfig{}
	err := cleanenv.ReadEnv(cfg)
	return cfg, err
}
