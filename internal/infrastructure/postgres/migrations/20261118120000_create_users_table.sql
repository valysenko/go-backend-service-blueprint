-- +goose Up
CREATE TABLE users (
    uuid UUID PRIMARY KEY,
    email TEXT NOT NULL,
    full_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_users_email_unique ON users (lower(email));
CREATE INDEX idx_users_created_at ON users (created_at DESC, uuid);

-- +goose Down
DROP TABLE IF EXISTS users;
