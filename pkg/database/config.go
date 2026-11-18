package database

import (
	"fmt"
	"net/url"
)

type DBConfig struct {
	Host           string
	Port           string
	Username       string
	Password       string
	DBName         string
	SSLMode        string
	MaxConns       int32
	MinConns       int32
	ConnectTimeout int
}

func (cfg *DBConfig) ProvideDSN() string {
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s&connect_timeout=%d",
		url.QueryEscape(cfg.Username),
		url.QueryEscape(cfg.Password),
		cfg.Host,
		cfg.Port,
		cfg.DBName,
		sslMode,
		cfg.ConnectTimeout)
}
