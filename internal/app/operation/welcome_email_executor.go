package operation

import (
	"context"
	"encoding/json"
	"errors"

	"go-backend-service-blueprint/internal/domain/operation"
	domainuser "go-backend-service-blueprint/internal/domain/user"
)

type WelcomeEmailExecutor struct {
	users  UserSource
	sender EmailSender
}

var _ Executor = (*WelcomeEmailExecutor)(nil)

func NewWelcomeEmailExecutor(users UserSource, sender EmailSender) *WelcomeEmailExecutor {
	return &WelcomeEmailExecutor{users: users, sender: sender}
}

func (e *WelcomeEmailExecutor) Execute(ctx context.Context, op *operation.Operation) (json.RawMessage, error) {
	recipient, err := e.users.GetByUUID(ctx, op.EntityID)
	if err != nil {
		if errors.Is(err, domainuser.ErrNotFound) {
			return nil, &operation.FailedError{Reason: operation.ReasonEntityNotFound}
		}

		return nil, &operation.RecoverableError{Cause: err}
	}

	if err := e.sender.SendWelcome(ctx, recipient.Email, recipient.FullName); err != nil {
		return nil, &operation.RecoverableError{Cause: err}
	}

	return json.Marshal(map[string]string{"sent_to": recipient.Email})
}
