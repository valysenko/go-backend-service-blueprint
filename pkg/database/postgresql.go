package database

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

const defaultPingTimeout = 5 * time.Second

type AppDB struct {
	Postgresql *pgxpool.Pool
}

func InitDB(dbConfig *DBConfig) (*AppDB, error) {
	poolConfig, err := pgxpool.ParseConfig(dbConfig.ProvideDSN())
	if err != nil {
		return nil, fmt.Errorf("parse pg config: %w", err)
	}

	if dbConfig.MaxConns > 0 {
		poolConfig.MaxConns = dbConfig.MaxConns
	}
	if dbConfig.MinConns > 0 {
		poolConfig.MinConns = dbConfig.MinConns
	}
	poolConfig.PingTimeout = time.Duration(dbConfig.ConnectTimeout) * time.Second

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	return &AppDB{Postgresql: pool}, nil
}

func (db *AppDB) Close() {
	db.Postgresql.Close()
}

func (db *AppDB) Name() string {
	return "database"
}

func (db *AppDB) Ping() error {
	timeout := db.Postgresql.Config().ConnConfig.ConnectTimeout
	if timeout <= 0 {
		timeout = defaultPingTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return db.Postgresql.Ping(ctx)
}

func (db *AppDB) BeginTransaction(ctx context.Context) (pgx.Tx, error) {
	return db.Postgresql.Begin(ctx)
}

func (db *AppDB) RunMigrations(path string) error {
	sqlDB := stdlib.OpenDB(*db.Postgresql.Config().ConnConfig)
	defer func() {
		if err := sqlDB.Close(); err != nil {
			slog.Error("failed to close migration connection", "error", err)
		}
	}()

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}
	if err := goose.Up(sqlDB, path); err != nil {
		return fmt.Errorf("run migrations from %q: %w", path, err)
	}

	return nil
}

func (db *AppDB) DownMigrations(path string) error {
	sqlDB := stdlib.OpenDB(*db.Postgresql.Config().ConnConfig)
	defer func() {
		if err := sqlDB.Close(); err != nil {
			slog.Error("failed to close migration connection", "error", err)
		}
	}()

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}
	if err := goose.DownTo(sqlDB, path, 0); err != nil {
		return fmt.Errorf("roll back migrations from %q: %w", path, err)
	}

	return nil
}
