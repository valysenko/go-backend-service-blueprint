package notification

import (
	"context"
	"log/slog"
)

type LogEmailSender struct {
	logger *slog.Logger
}

func NewLogEmailSender(logger *slog.Logger) *LogEmailSender {
	return &LogEmailSender{logger: logger}
}

func (s *LogEmailSender) SendWelcome(ctx context.Context, email, fullName string) error {
	s.logger.InfoContext(ctx, "welcome email sent", "email", email, "full_name", fullName)

	return nil
}
