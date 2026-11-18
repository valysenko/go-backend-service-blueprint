-- +goose Up
CREATE TABLE operations (
    uuid UUID PRIMARY KEY,
    operation_type SMALLINT NOT NULL,
    entity_id UUID NOT NULL,
    status SMALLINT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    result JSONB,
    planned_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_operations_planned ON operations (status, planned_at, uuid);
CREATE INDEX idx_operations_entity_id ON operations (entity_id);

-- +goose Down
DROP TABLE IF EXISTS operations;
