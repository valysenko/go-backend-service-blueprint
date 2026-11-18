package user

import (
	"time"

	appuser "go-backend-service-blueprint/internal/app/user"
	domainuser "go-backend-service-blueprint/internal/domain/user"

	"github.com/google/uuid"
)

type CreateUserRequest struct {
	Email    string `json:"email" validate:"required,email,max=255"`
	FullName string `json:"fullName" validate:"required,min=1,max=255"`
}

type UserResponse struct {
	ID        uuid.UUID `json:"id"`
	Email     string    `json:"email"`
	FullName  string    `json:"fullName"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type ListUsersResponse struct {
	Items       []UserResponse `json:"items"`
	CurrentPage int            `json:"currentPage"`
	Limit       int            `json:"limit"`
}

func newUserResponse(user *domainuser.User) UserResponse {
	return UserResponse{
		ID:        user.UUID,
		Email:     user.Email,
		FullName:  user.FullName,
		CreatedAt: user.CreatedAt,
		UpdatedAt: user.UpdatedAt,
	}
}

func newListUsersResponse(result *appuser.ListResult) ListUsersResponse {
	items := make([]UserResponse, 0, len(result.Items))
	for i := range result.Items {
		items = append(items, newUserResponse(&result.Items[i]))
	}

	return ListUsersResponse{
		Items:       items,
		CurrentPage: result.Page,
		Limit:       result.Limit,
	}
}
