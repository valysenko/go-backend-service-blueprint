package operation

import (
	"context"
	"encoding/json"

	"go-backend-service-blueprint/internal/domain/operation"
)

type Executor interface {
	Execute(ctx context.Context, op *operation.Operation) (json.RawMessage, error)
}

type Registry struct {
	executors map[operation.OperationType]Executor
}

func NewRegistry(executors map[operation.OperationType]Executor) *Registry {
	registered := make(map[operation.OperationType]Executor, len(executors))
	for operationType, executor := range executors {
		registered[operationType] = executor
	}

	return &Registry{executors: registered}
}

func (r *Registry) For(operationType operation.OperationType) (Executor, error) {
	executor, ok := r.executors[operationType]
	if !ok {
		return nil, &operation.FailedError{Reason: operation.ReasonUnknownOperationType}
	}

	return executor, nil
}

func (r *Registry) Types() []operation.OperationType {
	types := make([]operation.OperationType, 0, len(r.executors))
	for operationType := range r.executors {
		types = append(types, operationType)
	}

	return types
}
