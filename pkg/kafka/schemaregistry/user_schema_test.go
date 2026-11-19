package schemaregistry_test

import (
	"context"
	"os"
	"testing"

	"go-backend-service-blueprint/pkg/kafka/schemaregistry"

	"github.com/stretchr/testify/require"
)

type userSchemaRegistry string

func (s userSchemaRegistry) SchemaBySubjectVersion(_ context.Context, _ string, _ int) (string, error) {
	return string(s), nil
}

func TestUserSchema(t *testing.T) {
	schema, err := os.ReadFile("../../../deployments/kafka/schemas/example_user_topic-value.json")
	require.NoError(t, err)
	validator := schemaregistry.NewValidator(userSchemaRegistry(schema))
	for _, tc := range []struct {
		name, payload string
		valid         bool
	}{
		{"valid", `{"uuid":"4c979df6-0b4f-4d38-a06d-f54e2325890e","email":"test@example.com"}`, true},
		{"missing email", `{"uuid":"4c979df6-0b4f-4d38-a06d-f54e2325890e"}`, false},
		{"invalid uuid", `{"uuid":"not-a-uuid","email":"test@example.com"}`, false},
		{"invalid email", `{"uuid":"4c979df6-0b4f-4d38-a06d-f54e2325890e","email":"invalid"}`, false},
		{"extra field", `{"uuid":"4c979df6-0b4f-4d38-a06d-f54e2325890e","email":"test@example.com","extra":1}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate(context.Background(), "example_user_topic-value", 1, []byte(tc.payload))
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, schemaregistry.ErrPayloadInvalid)
			}
		})
	}
}
