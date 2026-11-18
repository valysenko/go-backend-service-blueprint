package user

import (
	"context"
	"strconv"
	"time"

	appuser "go-backend-service-blueprint/internal/app/user"
	domainuser "go-backend-service-blueprint/internal/domain/user"
	"go-backend-service-blueprint/internal/presentation"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

type UseCases interface {
	Create(ctx context.Context, cmd appuser.CreateCommand) (*domainuser.User, error)
	Get(ctx context.Context, id uuid.UUID) (*domainuser.User, error)
	List(ctx context.Context, query appuser.ListQuery) (*appuser.ListResult, error)
}

type Handler struct {
	useCases UseCases
	timeout  time.Duration
}

func NewHandler(useCases UseCases, timeout time.Duration) *Handler {
	return &Handler{useCases: useCases, timeout: timeout}
}

// Create handles POST /api/v1/users.
//
// @Summary      Create user
// @Description  Creates a user. The email is normalized and must be unique.
// @Tags         users
// @Accept       json
// @Produce      json
// @Param        request  body      user.CreateUserRequest  true  "User to create"
// @Success      201      {object}  user.UserResponse
// @Failure      400      {object}  presentation.ErrorResponse
// @Failure      409      {object}  presentation.ErrorResponse
// @Failure      429      {object}  presentation.ErrorResponse
// @Failure      500      {object}  presentation.ErrorResponse
// @Router       /api/v1/users [post]
func (h *Handler) Create(c fiber.Ctx) error {
	var req CreateUserRequest
	if err := c.Bind().WithoutAutoHandling().JSON(&req); err != nil {
		return presentation.BadRequest(c, presentation.BindErrorMessage(err))
	}

	ctx, cancel := context.WithTimeout(c.Context(), h.timeout)
	defer cancel()

	created, err := h.useCases.Create(ctx, appuser.CreateCommand{
		Email:    req.Email,
		FullName: req.FullName,
		ClientIP: c.IP(),
	})
	if err != nil {
		return writeDomainError(c, err)
	}

	return c.Status(fiber.StatusCreated).JSON(newUserResponse(created))
}

// Get handles GET /api/v1/users/{id}.
//
// @Summary      Get user
// @Tags         users
// @Produce      json
// @Param        id   path      string  true  "User UUID"  format(uuid)
// @Success      200  {object}  user.UserResponse
// @Failure      400  {object}  presentation.ErrorResponse
// @Failure      404  {object}  presentation.ErrorResponse
// @Failure      500  {object}  presentation.ErrorResponse
// @Router       /api/v1/users/{id} [get]
func (h *Handler) Get(c fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return presentation.BadRequest(c, "id must be a valid UUID")
	}

	ctx, cancel := context.WithTimeout(c.Context(), h.timeout)
	defer cancel()

	found, err := h.useCases.Get(ctx, id)
	if err != nil {
		return writeDomainError(c, err)
	}

	return c.Status(fiber.StatusOK).JSON(newUserResponse(found))
}

// List handles GET /api/v1/users.
//
// @Summary      List users
// @Description  Returns a page of users, newest first.
// @Tags         users
// @Produce      json
// @Param        page   query     int     false  "Page number"  minimum(1)
// @Param        limit  query     int     false  "Page size"    minimum(1)  maximum(100)
// @Success      200    {object}  user.ListUsersResponse
// @Failure      400    {object}  presentation.ErrorResponse
// @Failure      500    {object}  presentation.ErrorResponse
// @Router       /api/v1/users [get]
func (h *Handler) List(c fiber.Ctx) error {
	page, err := optionalInt(c, "page")
	if err != nil {
		return presentation.BadRequest(c, "invalid page")
	}
	limit, err := optionalInt(c, "limit")
	if err != nil {
		return presentation.BadRequest(c, "invalid limit")
	}

	ctx, cancel := context.WithTimeout(c.Context(), h.timeout)
	defer cancel()

	result, err := h.useCases.List(ctx, appuser.ListQuery{Page: page, Limit: limit})
	if err != nil {
		return writeDomainError(c, err)
	}

	return c.Status(fiber.StatusOK).JSON(newListUsersResponse(result))
}

func optionalInt(c fiber.Ctx, name string) (int, error) {
	raw := c.Query(name)
	if raw == "" {
		return 0, nil
	}

	return strconv.Atoi(raw)
}
