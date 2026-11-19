package schemaregistry

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
)

type Client struct {
	inner *sr.Client
}

func NewClient(baseURL string) (*Client, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("schema registry baseURL must not be empty")
	}
	c, err := sr.NewClient(sr.URLs(baseURL))
	if err != nil {
		return nil, fmt.Errorf("sr client: %w", err)
	}
	return &Client{inner: c}, nil
}

// doesn't need context cancellation logic - franz-go/pkg/sr manages it
func (c *Client) SchemaBySubjectVersion(ctx context.Context, subject string, version int) (string, error) {
	if subject == "" {
		return "", fmt.Errorf("subject must not be empty")
	}
	if version <= 0 {
		return "", fmt.Errorf("version must be > 0, got %d", version)
	}
	s, err := c.inner.SchemaByVersion(ctx, subject, version)
	if err != nil {
		return "", fmt.Errorf("fetch %s v%d: %w", subject, version, err)
	}
	return s.Schema.Schema, nil
}
