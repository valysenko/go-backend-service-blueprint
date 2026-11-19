package schemaregistry_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-backend-service-blueprint/pkg/kafka/schemaregistry"

	"github.com/stretchr/testify/assert"
)

const (
	testSubject = "test-topic-value"
	testVersion = 3
	testSchema  = `{"type":"object","properties":{"email":{"type":"string"}}}`
)

func TestSchemaBySubjectVersion(t *testing.T) {
	ctx := context.Background()

	t.Run("returns schema string on success", func(t *testing.T) {
		var calls int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			expectedPath := fmt.Sprintf("/subjects/%s/versions/%d", testSubject, testVersion)
			assert.Equal(t, expectedPath, r.URL.Path)

			w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
			_, _ = fmt.Fprintf(w, `{"subject":%q,"id":42,"version":%d,"schema":%q,"schemaType":"JSON"}`,
				testSubject, testVersion, testSchema)
		}))
		t.Cleanup(srv.Close)

		cl, err := schemaregistry.NewClient(srv.URL)
		assert.NoError(t, err)

		got, err := cl.SchemaBySubjectVersion(ctx, testSubject, testVersion)

		assert.NoError(t, err)
		assert.Equal(t, testSchema, got)
		assert.Equal(t, 1, calls)
	})

	t.Run("returns error on registry 404", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprint(w, `{"error_code":123,"message":"Version not found"}`)
		}))
		t.Cleanup(srv.Close)

		cl, err := schemaregistry.NewClient(srv.URL)
		assert.NoError(t, err)

		got, err := cl.SchemaBySubjectVersion(ctx, testSubject, testVersion)

		assert.Error(t, err)
		assert.Empty(t, got)
		assert.Contains(t, err.Error(), testSubject)
	})

	t.Run("returns error on registry 500", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		t.Cleanup(srv.Close)

		cl, err := schemaregistry.NewClient(srv.URL)
		assert.NoError(t, err)

		got, err := cl.SchemaBySubjectVersion(ctx, testSubject, testVersion)

		assert.Error(t, err)
		assert.Empty(t, got)
	})

	t.Run("rejects empty subject without HTTP call", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			t.Fatalf("registry should not be called: %s", r.URL.Path)
		}))
		t.Cleanup(srv.Close)

		cl, err := schemaregistry.NewClient(srv.URL)
		assert.NoError(t, err)

		got, err := cl.SchemaBySubjectVersion(ctx, "", testVersion)

		assert.Error(t, err)
		assert.Empty(t, got)
		assert.Contains(t, err.Error(), "subject")
	})

	t.Run("rejects version <= 0 without HTTP call", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			t.Fatalf("registry should not be called: %s", r.URL.Path)
		}))
		t.Cleanup(srv.Close)

		cl, err := schemaregistry.NewClient(srv.URL)
		assert.NoError(t, err)

		for _, v := range []int{0, -1, -42} {
			got, err := cl.SchemaBySubjectVersion(ctx, testSubject, v)
			assert.Error(t, err, "version %d", v)
			assert.Empty(t, got)
			assert.Contains(t, err.Error(), "version")
		}
	})
}
