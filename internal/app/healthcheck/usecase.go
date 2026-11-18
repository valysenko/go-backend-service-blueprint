package healthcheck

const (
	StatusHealthy   = "healthy"
	StatusUnhealthy = "unhealthy"
)

type Checker interface {
	Name() string
	Ping() error
}

type HealthcheckResult struct {
	HealthcheckResults []ServiceResult `json:"healthcheck_results,omitempty"`
}

func (hcr *HealthcheckResult) IsHealthy() bool {
	for _, res := range hcr.HealthcheckResults {
		if res.Status != StatusHealthy {
			return false
		}
	}

	return true
}

type ServiceResult struct {
	Service string `json:"service"`
	Status  string `json:"status"`
}

type Healthcheck struct {
	checkers []Checker
}

func NewHealthcheck(checkers ...Checker) *Healthcheck {
	return &Healthcheck{checkers: checkers}
}

func (hc *Healthcheck) GetResults() *HealthcheckResult {
	res := HealthcheckResult{
		HealthcheckResults: make([]ServiceResult, 0, len(hc.checkers)),
	}

	for _, checker := range hc.checkers {
		status := StatusHealthy
		if err := checker.Ping(); err != nil {
			status = StatusUnhealthy
		}
		res.HealthcheckResults = append(res.HealthcheckResults, ServiceResult{
			Service: checker.Name(),
			Status:  status,
		})
	}

	return &res
}
