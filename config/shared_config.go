package config

const (
	EnvLocal = "loc"
	EnvTest  = "test"
)

type ServerConfig struct {
	ServerPort     string `env:"SERVER_PORT" env-default:"8096"`
	ReadTimeout    int    `env:"READ_TIMEOUT" env-default:"60"`
	WriteTimeout   int    `env:"WRITE_TIMEOUT" env-default:"120"`
	RequestTimeout int    `env:"REQUEST_TIMEOUT" env-default:"10"`
	BodyLimit      int    `env:"BODY_LIMIT_BYTES" env-default:"5242880"`
}
