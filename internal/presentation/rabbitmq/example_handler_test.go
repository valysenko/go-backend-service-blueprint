package rabbitmq_test

import (
	"context"
	"errors"
	"testing"

	"go-backend-service-blueprint/internal/app/rabbitmqexample"
	example "go-backend-service-blueprint/internal/presentation/rabbitmq"
	"go-backend-service-blueprint/internal/presentation/rabbitmq/mocks"
	"go-backend-service-blueprint/pkg/rabbitmq"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestHandlerPassesCommandAndReturnsError(t *testing.T) {
	ctx := context.Background()
	useCase := mocks.NewMockUseCase(t)
	failure := errors.New("use case failed")
	useCase.EXPECT().Execute(ctx, rabbitmqexample.Command{Queue: "example.direct", Text: "hello", Attempt: 2}).Return(failure).Once()
	h := example.ExampleHandler{UseCase: useCase, Queue: "example.direct"}
	require.ErrorIs(t, h.Handle(ctx, rabbitmq.Delivery{Body: []byte(`{"text":"hello"}`), Attempt: 2}), failure)
}

func TestHandlerRejectsInvalidMessage(t *testing.T) {
	for _, body := range []string{`{`, `{"text":""}`} {
		t.Run(body, func(t *testing.T) {
			useCase := mocks.NewMockUseCase(t)
			h := example.ExampleHandler{UseCase: useCase}
			err := h.Handle(context.Background(), rabbitmq.Delivery{Body: []byte(body)})
			var permanent *rabbitmq.PermanentError
			require.ErrorAs(t, err, &permanent)
			useCase.AssertNotCalled(t, "Execute", mock.Anything, mock.Anything)
		})
	}
}
