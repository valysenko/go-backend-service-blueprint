package kafkaapp

import (
	"testing"

	"go-backend-service-blueprint/pkg/kafka/reader/handler"
	"go-backend-service-blueprint/pkg/kafka/reader/handler/mocks"
	"go-backend-service-blueprint/pkg/kafka/storage"

	"github.com/stretchr/testify/assert"
)

const (
	tcTopic      = "test-topic"
	tcGroup      = "test-group"
	tcDLTopic    = "test-topic-dl"
	tcDLGroup    = "test-group-dl"
	tcSubject    = "test-topic-value"
	tcVersion    = 1
	tcMaxRetries = 5
)

func validRetryConfig() handler.RetryConfig {
	return handler.MustRetryConfig(3, 100, 2)
}

func validDLSource() storage.DLSource {
	return storage.DLSource{Topic: tcTopic, ConsumerGroup: tcGroup}
}

// validBaseFlowConfig returns a config every field of which is valid, so each
// case below can invalidate exactly one thing.
func validBaseFlowConfig(t *testing.T) BaseFlowConfig {
	t.Helper()
	return BaseFlowConfig{
		Topic:         tcTopic,
		ConsumerGroup: tcGroup,
		Processor:     mocks.NewMockMessageProcessor(t),
		DLSource:      validDLSource(),
		DLTopic:       tcDLTopic,
		Retry:         validRetryConfig(),
		SchemaSubject: tcSubject,
		SchemaVersion: tcVersion,
	}
}

func validDLFlowConfig(t *testing.T) DLFlowConfig {
	t.Helper()
	return DLFlowConfig{
		Topic:         tcDLTopic,
		ConsumerGroup: tcDLGroup,
		Processor:     mocks.NewMockMessageProcessor(t),
		DLSource:      validDLSource(),
		MaxRetries:    tcMaxRetries,
		SchemaSubject: tcSubject,
		SchemaVersion: tcVersion,
	}
}

func TestNewBaseFlowTopicConsumer(t *testing.T) {
	t.Run("valid input returns BaseFlow consumer with all fields populated", func(t *testing.T) {
		c, err := NewBaseFlowTopicConsumer(validBaseFlowConfig(t))

		assert.NoError(t, err)
		assert.Equal(t, BaseFlow, c.Mode())
		assert.Equal(t, tcTopic, c.Topic)
		assert.Equal(t, tcGroup, c.ConsumerGroup)
		assert.Equal(t, tcDLTopic, c.DLTopic)
		assert.Equal(t, validDLSource(), c.DLSource)
		assert.Equal(t, validRetryConfig(), c.RetryConfig)
		assert.Equal(t, tcSubject, c.SchemaSubject)
		assert.Equal(t, tcVersion, c.SchemaVersion)
	})

	t.Run("empty topic returns error", func(t *testing.T) {
		cfg := validBaseFlowConfig(t)
		cfg.Topic = ""

		_, err := NewBaseFlowTopicConsumer(cfg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "common fields")
	})

	t.Run("empty group returns error", func(t *testing.T) {
		cfg := validBaseFlowConfig(t)
		cfg.ConsumerGroup = ""

		_, err := NewBaseFlowTopicConsumer(cfg)

		assert.Error(t, err)
	})

	t.Run("nil processor returns error", func(t *testing.T) {
		cfg := validBaseFlowConfig(t)
		cfg.Processor = nil

		_, err := NewBaseFlowTopicConsumer(cfg)

		assert.Error(t, err)
	})

	t.Run("empty dlTopic returns error", func(t *testing.T) {
		cfg := validBaseFlowConfig(t)
		cfg.DLTopic = ""

		_, err := NewBaseFlowTopicConsumer(cfg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "dlTopic")
	})

	t.Run("zero retryConfig returns error", func(t *testing.T) {
		cfg := validBaseFlowConfig(t)
		cfg.Retry = handler.RetryConfig{}

		_, err := NewBaseFlowTopicConsumer(cfg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "retryConfig")
	})

	t.Run("schema subject set without version returns error", func(t *testing.T) {
		cfg := validBaseFlowConfig(t)
		cfg.SchemaVersion = 0

		_, err := NewBaseFlowTopicConsumer(cfg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SchemaVersion")
	})

	t.Run("schema disabled (both subject empty and version 0) is valid", func(t *testing.T) {
		cfg := validBaseFlowConfig(t)
		cfg.SchemaSubject = ""
		cfg.SchemaVersion = 0

		c, err := NewBaseFlowTopicConsumer(cfg)

		assert.NoError(t, err)
		assert.Empty(t, c.SchemaSubject)
		assert.Zero(t, c.SchemaVersion)
	})
}

func TestNewDLFlowTopicConsumer(t *testing.T) {
	t.Run("valid input returns DLFlow consumer with all fields populated", func(t *testing.T) {
		c, err := NewDLFlowTopicConsumer(validDLFlowConfig(t))

		assert.NoError(t, err)
		assert.Equal(t, DLFlow, c.Mode())
		assert.Equal(t, tcDLTopic, c.Topic)
		assert.Equal(t, tcDLGroup, c.ConsumerGroup)
		assert.Equal(t, tcMaxRetries, c.DLMaxRetries)
		assert.Equal(t, validDLSource(), c.DLSource,
			"the DL entry's DLSource must still name the BASE topic/group, not its own")
	})

	t.Run("missing common fields returns error", func(t *testing.T) {
		cfg := validDLFlowConfig(t)
		cfg.Topic = ""
		cfg.Processor = nil

		_, err := NewDLFlowTopicConsumer(cfg)

		assert.Error(t, err)
	})

	t.Run("negative maxRetries returns error", func(t *testing.T) {
		cfg := validDLFlowConfig(t)
		cfg.MaxRetries = -1

		_, err := NewDLFlowTopicConsumer(cfg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "maxRetries")
	})

	t.Run("zero maxRetries is valid (= retry forever)", func(t *testing.T) {
		cfg := validDLFlowConfig(t)
		cfg.MaxRetries = 0

		c, err := NewDLFlowTopicConsumer(cfg)

		assert.NoError(t, err)
		assert.Zero(t, c.DLMaxRetries)
	})

	t.Run("schema subject set without version returns error", func(t *testing.T) {
		cfg := validDLFlowConfig(t)
		cfg.SchemaVersion = 0

		_, err := NewDLFlowTopicConsumer(cfg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SchemaVersion")
	})
}

func TestMustBaseFlowTopicConsumer(t *testing.T) {
	t.Run("returns consumer on valid input", func(t *testing.T) {
		c := MustBaseFlowTopicConsumer(validBaseFlowConfig(t))

		assert.Equal(t, BaseFlow, c.Mode())
	})

	t.Run("panics on invalid input", func(t *testing.T) {
		cfg := validBaseFlowConfig(t)
		cfg.Topic = ""

		assert.Panics(t, func() { MustBaseFlowTopicConsumer(cfg) })
	})
}

func TestMustDLFlowTopicConsumer(t *testing.T) {
	t.Run("returns consumer on valid input", func(t *testing.T) {
		c := MustDLFlowTopicConsumer(validDLFlowConfig(t))

		assert.Equal(t, DLFlow, c.Mode())
	})

	t.Run("panics on invalid input", func(t *testing.T) {
		cfg := validDLFlowConfig(t)
		cfg.Processor = nil

		assert.Panics(t, func() { MustDLFlowTopicConsumer(cfg) })
	})
}

// A duplicate [topic + consumer group] pair is the one mistake the per-entry
// constructors cannot see and the per-binary topics_test.go misses too: those
// index entries by topic, so a duplicate silently overwrites.
func TestValidateConsumerSet(t *testing.T) {
	t.Run("distinct topic+group pairs are valid", func(t *testing.T) {
		consumers := []TopicConsumer{
			MustBaseFlowTopicConsumer(validBaseFlowConfig(t)),
			MustDLFlowTopicConsumer(validDLFlowConfig(t)),
		}

		assert.NoError(t, validateConsumerSet(consumers))
	})

	t.Run("same topic on different groups is valid", func(t *testing.T) {
		second := validBaseFlowConfig(t)
		second.ConsumerGroup = "other-group"

		consumers := []TopicConsumer{
			MustBaseFlowTopicConsumer(validBaseFlowConfig(t)),
			MustBaseFlowTopicConsumer(second),
		}

		assert.NoError(t, validateConsumerSet(consumers))
	})

	t.Run("duplicate topic+group pair returns error", func(t *testing.T) {
		consumers := []TopicConsumer{
			MustBaseFlowTopicConsumer(validBaseFlowConfig(t)),
			MustBaseFlowTopicConsumer(validBaseFlowConfig(t)),
		}

		err := validateConsumerSet(consumers)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate consumer")
	})
}
