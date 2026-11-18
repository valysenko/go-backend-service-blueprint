package hello

import "github.com/gofiber/fiber/v3"

type Response struct {
	Message string `json:"message"`
}

type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

// Hello @Summary      Hello world
// @Description  Minimal endpoint demonstrating the presentation layer.
// @Tags         hello
// @Produce      json
// @Success      200  {object}  hello.Response
// @Router       /hello [get]
func (h *Handler) Hello(c fiber.Ctx) error {
	return c.JSON(Response{Message: "hello world"})
}
