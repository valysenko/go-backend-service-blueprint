package config

type PostgreSQLConfig struct {
	Host           string `env:"DB_HOST"`
	Port           string `env:"DB_PORT" env-default:"5432"`
	Username       string `env:"DB_USERNAME"`
	Password       string `env:"DB_PASSWORD"`
	DBName         string `env:"DB_NAME"`
	SSLMode        string `env:"DB_SSL_MODE" env-default:"disable"`
	MaxConns       int32  `env:"DB_MAX_CONNS" env-default:"10"`
	MinConns       int32  `env:"DB_MIN_CONNS" env-default:"2"`
	ConnectTimeout int    `env:"DB_CONNECT_TIMEOUT" env-default:"5"`
	// The container image copies migrations next to the binaries, so the
	// default is relative to the working directory of the final-prod image.
	MigrationsDir string `env:"DB_MIGRATIONS_DIR" env-default:"./migrations"`
}
