package postgres

import (
	"context"

	"go-backend-service-blueprint/internal/app/tx"
	"go-backend-service-blueprint/pkg/database"

	"github.com/jackc/pgx/v5"
)

type TxManager struct {
	db *database.AppDB
}

var _ tx.Manager = (*TxManager)(nil)

func NewTxManager(db *database.AppDB) *TxManager {
	return &TxManager{db: db}
}

func (m *TxManager) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return fn(ctx)
	}

	return pgx.BeginFunc(ctx, m.db.Postgresql, func(t pgx.Tx) error {
		return fn(context.WithValue(ctx, txKey{}, t))
	})
}
