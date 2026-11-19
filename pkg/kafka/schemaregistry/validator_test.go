package schemaregistry_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go-backend-service-blueprint/pkg/kafka/schemaregistry"
	"go-backend-service-blueprint/pkg/kafka/schemaregistry/mocks"

	"github.com/stretchr/testify/assert"
)

const (
	validateSubject = "user-value"
	validateVersion = 1
	userSchemaV1    = `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"email": {"type": "string"},
			"age":   {"type": "integer", "minimum": 0}
		},
		"required": ["email"]
	}`
)

func TestValidator_Validate(t *testing.T) {
	ctx := context.Background()

	t.Run("valid payload returns nil", func(t *testing.T) {
		reg := mocks.NewMockRegistry(t)
		reg.EXPECT().SchemaBySubjectVersion(ctx, validateSubject, validateVersion).Return(userSchemaV1, nil).Once()
		v := schemaregistry.NewValidator(reg)

		err := v.Validate(ctx, validateSubject, validateVersion, []byte(`{"email":"a@b.com","age":30}`))

		assert.NoError(t, err)
	})

	t.Run("schema mismatch wraps ErrPayloadInvalid", func(t *testing.T) {
		reg := mocks.NewMockRegistry(t)
		reg.EXPECT().SchemaBySubjectVersion(ctx, validateSubject, validateVersion).Return(userSchemaV1, nil).Once()
		v := schemaregistry.NewValidator(reg)

		err := v.Validate(ctx, validateSubject, validateVersion, []byte(`{"age":-5}`))

		assert.Error(t, err)
		assert.ErrorIs(t, err, schemaregistry.ErrPayloadInvalid)
	})

	t.Run("invalid payload json wraps ErrPayloadInvalid", func(t *testing.T) {
		reg := mocks.NewMockRegistry(t)
		reg.EXPECT().SchemaBySubjectVersion(ctx, validateSubject, validateVersion).Return(userSchemaV1, nil).Once()
		v := schemaregistry.NewValidator(reg)

		err := v.Validate(ctx, validateSubject, validateVersion, []byte(`not json`))

		assert.Error(t, err)
		assert.ErrorIs(t, err, schemaregistry.ErrPayloadInvalid)
	})

	t.Run("registry error is NOT ErrPayloadInvalid", func(t *testing.T) {
		registryErr := errors.New("registry down")
		reg := mocks.NewMockRegistry(t)
		reg.EXPECT().SchemaBySubjectVersion(ctx, validateSubject, validateVersion).Return("", registryErr).Once()
		v := schemaregistry.NewValidator(reg)

		err := v.Validate(ctx, validateSubject, validateVersion, []byte(`{"email":"a@b.com"}`))

		assert.Error(t, err)
		assert.ErrorIs(t, err, registryErr)
		assert.NotErrorIs(t, err, schemaregistry.ErrPayloadInvalid)
	})

	t.Run("unparseable schema from registry is NOT ErrPayloadInvalid", func(t *testing.T) {
		reg := mocks.NewMockRegistry(t)
		reg.EXPECT().SchemaBySubjectVersion(ctx, validateSubject, validateVersion).Return("not-json", nil).Once()
		v := schemaregistry.NewValidator(reg)

		err := v.Validate(ctx, validateSubject, validateVersion, []byte(`{"email":"a@b.com"}`))

		assert.Error(t, err)
		assert.NotErrorIs(t, err, schemaregistry.ErrPayloadInvalid)
	})

	t.Run("invalid uncompilable schema from registry is NOT ErrPayloadInvalid", func(t *testing.T) {
		// Valid JSON but invalid JSON Schema (type must be string/array, not int).
		reg := mocks.NewMockRegistry(t)
		reg.EXPECT().SchemaBySubjectVersion(ctx, validateSubject, validateVersion).Return(`{"type": 42}`, nil).Once()
		v := schemaregistry.NewValidator(reg)

		err := v.Validate(ctx, validateSubject, validateVersion, []byte(`{}`))

		assert.Error(t, err)
		assert.NotErrorIs(t, err, schemaregistry.ErrPayloadInvalid)
	})
}

func TestValidator_Cache(t *testing.T) {
	ctx := context.Background()

	t.Run("repeated calls hit registry once per (subject,version)", func(t *testing.T) {
		reg := mocks.NewMockRegistry(t)
		// .Once() asserts exactly one call across all 5 Validate invocations.
		reg.EXPECT().SchemaBySubjectVersion(ctx, validateSubject, validateVersion).Return(userSchemaV1, nil).Once()
		v := schemaregistry.NewValidator(reg)

		for i := 0; i < 5; i++ {
			assert.NoError(t, v.Validate(ctx, validateSubject, validateVersion, []byte(`{"email":"a@b.com"}`)))
		}
	})

	t.Run("different keys are cached separately", func(t *testing.T) {
		reg := mocks.NewMockRegistry(t)
		reg.EXPECT().SchemaBySubjectVersion(ctx, "user-value", 1).Return(userSchemaV1, nil).Once()
		reg.EXPECT().SchemaBySubjectVersion(ctx, "user-value", 2).Return(userSchemaV1, nil).Once()
		reg.EXPECT().SchemaBySubjectVersion(ctx, "order-value", 1).Return(userSchemaV1, nil).Once()
		v := schemaregistry.NewValidator(reg)

		assert.NoError(t, v.Validate(ctx, "user-value", 1, []byte(`{"email":"a@b.com"}`)))
		assert.NoError(t, v.Validate(ctx, "user-value", 2, []byte(`{"email":"a@b.com"}`)))
		assert.NoError(t, v.Validate(ctx, "order-value", 1, []byte(`{"email":"a@b.com"}`)))
	})

	t.Run("registry error is not cached: next call retries", func(t *testing.T) {
		registryErr := errors.New("registry down")
		reg := mocks.NewMockRegistry(t)
		// Two calls expected: a failed fetch must not poison the cache.
		reg.EXPECT().SchemaBySubjectVersion(ctx, validateSubject, validateVersion).Return("", registryErr).Times(2)
		v := schemaregistry.NewValidator(reg)

		_ = v.Validate(ctx, validateSubject, validateVersion, []byte(`{}`))
		_ = v.Validate(ctx, validateSubject, validateVersion, []byte(`{}`))
	})
}

func TestValidator_Singleflight(t *testing.T) {
	ctx := context.Background()

	t.Run("concurrent calls for same key dedupe to one registry fetch", func(t *testing.T) {
		reg := mocks.NewMockRegistry(t)
		// .Once() — if singleflight failed and any follower also dialed the
		// registry, the second call would trip testify's strict mode and fail.
		reg.EXPECT().
			SchemaBySubjectVersion(ctx, validateSubject, validateVersion).
			RunAndReturn(func(context.Context, string, int) (string, error) {
				// Simulate a registry call, so concurrent callers can overlap.
				time.Sleep(30 * time.Millisecond)
				return userSchemaV1, nil
			}).
			Once()
		v := schemaregistry.NewValidator(reg)

		const goroutines = 10
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				_ = v.Validate(ctx, validateSubject, validateVersion, []byte(`{"email":"a@b.com"}`))
			}()
		}
		wg.Wait()
	})
}
