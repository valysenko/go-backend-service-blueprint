package schemaregistry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/sync/singleflight"
)

type Registry interface {
	SchemaBySubjectVersion(ctx context.Context, subject string, version int) (string, error)
}

// ErrPayloadInvalid means the message payload is invalid: not valid
// JSON, or valid JSON that fails schema validation.
var ErrPayloadInvalid = errors.New("payload invalid")

type Validator struct {
	registry Registry

	mu       sync.RWMutex
	compiled map[string]*jsonschema.Schema // key = "subject:version"

	// see https://pkg.go.dev/golang.org/x/sync/singleflight
	// see https://victoriametrics.com/blog/go-singleflight/
	sf singleflight.Group
}

func NewValidator(registry Registry) *Validator {
	if registry == nil {
		panic("schemaregistry: nil Registry")
	}

	return &Validator{
		registry: registry,
		compiled: make(map[string]*jsonschema.Schema),
	}
}

func (v *Validator) Validate(ctx context.Context, subject string, version int, payload []byte) error {
	schema, err := v.compiledFor(ctx, subject, version)
	if err != nil {
		return err
	}

	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("%w: invalid json: %w", ErrPayloadInvalid, err)
	}

	if err := schema.Validate(doc); err != nil {
		return fmt.Errorf("%w: schema %s v%d: %w", ErrPayloadInvalid, subject, version, err)
	}
	return nil
}

func (v *Validator) compiledFor(ctx context.Context, subject string, version int) (*jsonschema.Schema, error) {
	key := fmt.Sprintf("%s:%d", subject, version)

	// safe lookup against concurrent writers
	v.mu.RLock()
	if s, ok := v.compiled[key]; ok {
		v.mu.RUnlock()
		return s, nil
	}
	v.mu.RUnlock()

	// prevents concurretnt http calls to schema registry in case N goroutines get concrete schema which is not in cache
	// If other goroutines ask for the same data (same key) while that operation is still going, they’ll just wait.
	// Then, when the first one finishes, all the others get the same result without having to run the operation again.
	//
	// Note on ctx: sf.Do runs the leader's closure under the leader's ctx, so a leader cancellation
	// propagates the same error to all followers. This is intentional in this codebase — all callers
	// share a single root ctx derived from KafkaApp.Run, so cancellation always means "shutdown" and
	// followers stopping together is correct. If per-caller deadlines are ever introduced, switch to
	// sf.DoChan with a service-scoped ctx for the closure and per-caller select on the channel.
	res, err, _ := v.sf.Do(key, func() (any, error) {
		raw, err := v.registry.SchemaBySubjectVersion(ctx, subject, version)
		if err != nil {
			return nil, err
		}

		schemaDoc, err := jsonschema.UnmarshalJSON(strings.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("parse schema %s: %w", key, err)
		}

		c := jsonschema.NewCompiler()
		c.DefaultDraft(jsonschema.Draft7)
		c.AssertFormat()

		resourceURL := "mem://" + key
		if err := c.AddResource(resourceURL, schemaDoc); err != nil {
			return nil, fmt.Errorf("add resource %s: %w", key, err)
		}
		s, err := c.Compile(resourceURL)
		if err != nil {
			return nil, fmt.Errorf("compile %s: %w", key, err)
		}

		v.mu.Lock()
		v.compiled[key] = s
		v.mu.Unlock()
		return s, nil
	})
	if err != nil {
		return nil, err
	}
	return res.(*jsonschema.Schema), nil
}
