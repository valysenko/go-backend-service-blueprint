package healthcheck

import (
	apphc "go-backend-service-blueprint/internal/app/healthcheck"

	"github.com/gofiber/fiber/v3"
)

type CheckHealthUseCase interface {
	GetResults() *apphc.HealthcheckResult
}

type Handler struct {
	healthcheck CheckHealthUseCase
}

func NewHandler(hc CheckHealthUseCase) *Handler {
	return &Handler{healthcheck: hc}
}

// Check @Summary      Healthcheck
// @Description  Reports the liveness of the app and its backing services.
// @Tags         healthcheck
// @Produce      json
// @Success      200  {object}  healthcheck.HealthcheckResult
// @Failure      500  {object}  healthcheck.HealthcheckResult
// @Router       /healthcheck/ [get]
func (h *Handler) Check(c fiber.Ctx) error {
	res := h.healthcheck.GetResults()
	if !res.IsHealthy() {
		c.Status(fiber.StatusInternalServerError)
	}

	return c.JSON(res)
}
