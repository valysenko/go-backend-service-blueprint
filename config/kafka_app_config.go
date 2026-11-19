package config

import "github.com/ilyakaznacheev/cleanenv"

type KafkaAppConfig struct {
	RedisConfig
	Brokers           []string `env:"KAFKA_BROKERS" env-default:"blueprint-kafka:29092" env-separator:","`
	SchemaRegistryURL string   `env:"SCHEMA_REGISTRY_URL" env-default:"http://blueprint-schema-registry:8081"`
	MonitoringPort    string   `env:"KAFKA_MONITORING_PORT" env-default:"8097"`
}

func LoadKafkaAppConfig() (*KafkaAppConfig, error) {
	c := &KafkaAppConfig{}
	err := cleanenv.ReadEnv(c)
	return c, err
}
