package monitoring

type HTTPMetricsRecorder interface {
	IncrementRequestsTotal(method, endpoint, statusCode string)
	ObserveRequestDuration(method, endpoint, statusCode string, durationSeconds float64)
}
