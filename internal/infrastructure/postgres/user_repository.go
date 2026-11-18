package postgres

import (
	"context"
	"errors"
	"fmt"

	domainuser "go-backend-service-blueprint/internal/domain/user"
	"go-backend-service-blueprint/pkg/database"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	userColumns          = "uuid, email, full_name, created_at, updated_at"
	userEmailUniqueIndex = "idx_users_email_unique"
	pgUniqueViolation    = "23505"
)

type UserRepository struct {
	database *database.AppDB
}

var _ domainuser.Repository = (*UserRepository)(nil)

func NewUserRepository(db *database.AppDB) *UserRepository {
	return &UserRepository{database: db}
}

func (r *UserRepository) Create(ctx context.Context, user *domainuser.User) error {
	query := `INSERT INTO users (` + userColumns + `) VALUES ($1, $2, $3, $4, $5)`

	_, err := r.database.Postgresql.Exec(ctx, query,
		user.UUID, user.Email, user.FullName, user.CreatedAt, user.UpdatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation && pgErr.ConstraintName == userEmailUniqueIndex {
			return fmt.Errorf("%w: email=%s", domainuser.ErrAlreadyExists, user.Email)
		}

		return fmt.Errorf("insert user %s: %w", user.UUID, err)
	}

	return nil
}

func (r *UserRepository) GetByUUID(ctx context.Context, id uuid.UUID) (*domainuser.User, error) {
	query := `SELECT ` + userColumns + ` FROM users WHERE uuid = $1`

	var user domainuser.User
	err := r.database.Postgresql.QueryRow(ctx, query, id).
		Scan(&user.UUID, &user.Email, &user.FullName, &user.CreatedAt, &user.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: uuid=%s", domainuser.ErrNotFound, id)
		}

		return nil, fmt.Errorf("select user %s: %w", id, err)
	}

	return &user, nil
}

func (r *UserRepository) List(ctx context.Context, filter domainuser.ListFilter) ([]domainuser.User, error) {
	query := `SELECT ` + userColumns + ` FROM users
		ORDER BY created_at DESC, uuid
		LIMIT $1 OFFSET $2`

	rows, err := r.database.Postgresql.Query(ctx, query, filter.Limit, filter.Offset)
	if err != nil {
		return nil, fmt.Errorf("select users: %w", err)
	}
	defer rows.Close()

	items := make([]domainuser.User, 0, filter.Limit)
	for rows.Next() {
		var user domainuser.User
		if err := rows.Scan(&user.UUID, &user.Email, &user.FullName, &user.CreatedAt, &user.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		items = append(items, user)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate users: %w", err)
	}

	return items, nil
}
