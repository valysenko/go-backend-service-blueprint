package user_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	appuser "go-backend-service-blueprint/internal/app/user"
	domainuser "go-backend-service-blueprint/internal/domain/user"
	"go-backend-service-blueprint/internal/presentation"
	presentationuser "go-backend-service-blueprint/internal/presentation/http/user"
	"go-backend-service-blueprint/internal/presentation/http/user/mocks"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func newTestServer(useCases presentationuser.UseCases) *fiber.App {
	app := fiber.New(fiber.Config{StructValidator: presentation.NewStructValidator()})
	handler := presentationuser.NewHandler(useCases, time.Second)

	app.Post("/api/v1/users", handler.Create)
	app.Get("/api/v1/users", handler.List)
	app.Get("/api/v1/users/:id", handler.Get)

	return app
}

func post(t *testing.T, app *fiber.App, path, body string) (*http.Response, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)

	return do(t, app, req)
}

func get(t *testing.T, app *fiber.App, path string) (*http.Response, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	require.NoError(t, err)

	return do(t, app, req)
}

func do(t *testing.T, app *fiber.App, req *http.Request) (*http.Response, []byte) {
	t.Helper()

	resp, err := app.Test(req)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	payload, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp, payload
}

func TestCreateReturnsCreatedUser(t *testing.T) {
	created := &domainuser.User{UUID: uuid.New(), Email: "jane@example.com", FullName: "Jane"}

	useCases := mocks.NewMockUseCases(t)
	useCases.EXPECT().
		Create(mock.Anything, mock.MatchedBy(func(cmd appuser.CreateCommand) bool {
			return cmd.Email == "Jane@Example.com" && cmd.FullName == "Jane" && cmd.ClientIP != ""
		})).
		Return(created, nil).
		Once()

	resp, body := post(t, newTestServer(useCases), "/api/v1/users", `{"email":"Jane@Example.com","fullName":"Jane"}`)

	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var out presentationuser.UserResponse
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Equal(t, created.UUID, out.ID)
	assert.Equal(t, "jane@example.com", out.Email)
}

func TestCreateRejectsInvalidEmail(t *testing.T) {
	resp, body := post(t, newTestServer(mocks.NewMockUseCases(t)), "/api/v1/users", `{"email":"not-an-email","fullName":"Jane"}`)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "Email")
}

func TestCreateMapsAlreadyExistsToConflict(t *testing.T) {
	useCases := mocks.NewMockUseCases(t)
	useCases.EXPECT().Create(mock.Anything, mock.Anything).Return(nil, domainuser.ErrAlreadyExists).Once()

	resp, _ := post(t, newTestServer(useCases), "/api/v1/users", `{"email":"jane@example.com","fullName":"Jane"}`)

	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestCreateMapsRateLimitTo429WithRetryAfter(t *testing.T) {
	useCases := mocks.NewMockUseCases(t)
	useCases.EXPECT().
		Create(mock.Anything, mock.Anything).
		Return(nil, &appuser.RateLimitedError{RetryAfter: 1500 * time.Millisecond}).
		Once()

	resp, body := post(t, newTestServer(useCases), "/api/v1/users", `{"email":"jane@example.com","fullName":"Jane"}`)

	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, "2", resp.Header.Get(fiber.HeaderRetryAfter))
	assert.Contains(t, string(body), "too many requests")
}

func TestGetReturnsUser(t *testing.T) {
	found := &domainuser.User{UUID: uuid.New(), Email: "jane@example.com", FullName: "Jane"}

	useCases := mocks.NewMockUseCases(t)
	useCases.EXPECT().Get(mock.Anything, found.UUID).Return(found, nil).Once()

	resp, body := get(t, newTestServer(useCases), "/api/v1/users/"+found.UUID.String())

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out presentationuser.UserResponse
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Equal(t, found.UUID, out.ID)
}

func TestGetMapsNotFoundTo404(t *testing.T) {
	useCases := mocks.NewMockUseCases(t)
	useCases.EXPECT().Get(mock.Anything, mock.Anything).Return(nil, domainuser.ErrNotFound).Once()

	resp, _ := get(t, newTestServer(useCases), "/api/v1/users/"+uuid.NewString())

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGetRejectsMalformedUUID(t *testing.T) {
	resp, _ := get(t, newTestServer(mocks.NewMockUseCases(t)), "/api/v1/users/not-a-uuid")

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestListReturnsPageMeta(t *testing.T) {
	useCases := mocks.NewMockUseCases(t)
	useCases.EXPECT().
		List(mock.Anything, appuser.ListQuery{Page: 2, Limit: 10}).
		Return(&appuser.ListResult{
			Items: []domainuser.User{{UUID: uuid.New(), Email: "a@example.com"}},
			Page:  2,
			Limit: 10,
		}, nil).
		Once()

	resp, body := get(t, newTestServer(useCases), "/api/v1/users?page=2&limit=10")

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out presentationuser.ListUsersResponse
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Equal(t, 2, out.CurrentPage)
	assert.Equal(t, 10, out.Limit)
	assert.Len(t, out.Items, 1)
}

func TestListRejectsNonNumericPage(t *testing.T) {
	resp, _ := get(t, newTestServer(mocks.NewMockUseCases(t)), "/api/v1/users?page=abc")

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
