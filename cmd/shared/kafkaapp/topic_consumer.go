package kafkaapp

import (
	"fmt"

	"go-backend-service-blueprint/pkg/kafka/reader/handler"
	"go-backend-service-blueprint/pkg/kafka/storage"
)

type FlowMode int

const (
	BaseFlow FlowMode = iota
	DLFlow
)

const (
	DefaultConsumerGroup   = "user_management"
	DefaultDLConsumerGroup = "user_management_service_dl"
)

// TopicConsumer describes a single Kafka topic consumer entry [topic+consumer group].
// - NewBaseFlowTopicConsumer / MustBaseFlowTopicConsumer - for base topics
// - NewDLFlowTopicConsumer / MustDLFlowTopicConsumer     - for dl topics
// the unexported mode field guarantees the struct cannot be in an inconsistent state
type TopicConsumer struct {
	mode FlowMode

	Topic         string
	ConsumerGroup string
	Processor     handler.MessageProcessor
	DLSource      storage.DLSource

	// Schema validation
	// SchemaSubject == "" disables validation. when set, SchemaVersion must be > 0
	SchemaSubject string
	SchemaVersion int

	// Set on BaseFlow entries only.
	RetryConfig handler.RetryConfig
	DLTopic     string

	// Set on DLFlow entries only.
	// 0 = retry the inner DL processor forever (partition stalls on poison messages);
	// >0 = bounded retries, then drop and decrement counter so BaseFlow can resume.
	DLMaxRetries int
}

func (c TopicConsumer) Mode() FlowMode { return c.mode }

type BaseFlowConfig struct {
	Topic         string
	ConsumerGroup string
	Processor     handler.MessageProcessor

	// DLSource keys the per-partition-key DL counter in Redis
	// (dl_counter_{topic}_{group}_{key}). BaseFlow increments it when it
	// dead-letters a key and refuses to process that key inline while it is
	// > 0, which is what keeps a key's events in order across the DL detour.
	DLSource storage.DLSource

	DLTopic string
	Retry   handler.RetryConfig

	SchemaSubject string
	SchemaVersion int
}

// DLFlowConfig is the argument object for a DLFlow entry. See BaseFlowConfig
// for why these are named fields.
type DLFlowConfig struct {
	Topic         string
	ConsumerGroup string
	Processor     handler.MessageProcessor

	// DLSource must name the BASE topic and group, not this entry's own: this
	// flow decrements the counter BaseFlow incremented, and a mismatch means a
	// different Redis key. The base counter then never returns to 0 and every
	// later message for that key is dead-lettered unprocessed, silently.
	DLSource storage.DLSource

	// MaxRetries: 0 = retry the inner DL processor forever (the partition
	// stalls on a poison message); >0 = bounded retries, then drop and
	// decrement the counter so BaseFlow can resume.
	MaxRetries int

	SchemaSubject string
	SchemaVersion int
}

func NewBaseFlowTopicConsumer(c BaseFlowConfig) (TopicConsumer, error) {
	if err := validateCommonFields(c.Topic, c.ConsumerGroup, c.Processor); err != nil {
		return TopicConsumer{}, err
	}
	if err := validateBaseFlowFields(c.Topic, c.DLTopic, c.Retry); err != nil {
		return TopicConsumer{}, err
	}
	if err := validateSchemaFields(c.Topic, c.SchemaSubject, c.SchemaVersion); err != nil {
		return TopicConsumer{}, err
	}
	return TopicConsumer{
		mode:          BaseFlow,
		Topic:         c.Topic,
		ConsumerGroup: c.ConsumerGroup,
		Processor:     c.Processor,
		DLSource:      c.DLSource,
		SchemaSubject: c.SchemaSubject,
		SchemaVersion: c.SchemaVersion,
		RetryConfig:   c.Retry,
		DLTopic:       c.DLTopic,
	}, nil
}

func MustBaseFlowTopicConsumer(c BaseFlowConfig) TopicConsumer {
	tc, err := NewBaseFlowTopicConsumer(c)
	if err != nil {
		panic(err)
	}
	return tc
}

func NewDLFlowTopicConsumer(c DLFlowConfig) (TopicConsumer, error) {
	if err := validateCommonFields(c.Topic, c.ConsumerGroup, c.Processor); err != nil {
		return TopicConsumer{}, err
	}
	if err := validateDLFlowFields(c.Topic, c.MaxRetries); err != nil {
		return TopicConsumer{}, err
	}
	if err := validateSchemaFields(c.Topic, c.SchemaSubject, c.SchemaVersion); err != nil {
		return TopicConsumer{}, err
	}
	return TopicConsumer{
		mode:          DLFlow,
		Topic:         c.Topic,
		ConsumerGroup: c.ConsumerGroup,
		Processor:     c.Processor,
		DLSource:      c.DLSource,
		SchemaSubject: c.SchemaSubject,
		SchemaVersion: c.SchemaVersion,
		DLMaxRetries:  c.MaxRetries,
	}, nil
}

func MustDLFlowTopicConsumer(c DLFlowConfig) TopicConsumer {
	tc, err := NewDLFlowTopicConsumer(c)
	if err != nil {
		panic(err)
	}
	return tc
}

func validateCommonFields(topic, group string, processor handler.MessageProcessor) error {
	if topic == "" || group == "" || processor == nil {
		return fmt.Errorf("topic %q: required common fields missing", topic)
	}
	return nil
}

func validateBaseFlowFields(topic, dlTopic string, retryConfig handler.RetryConfig) error {
	if dlTopic == "" {
		return fmt.Errorf("topic %q: dlTopic required for BaseFlow", topic)
	}
	if retryConfig == (handler.RetryConfig{}) {
		return fmt.Errorf("topic %q: retryConfig required for BaseFlow", topic)
	}
	return nil
}

func validateDLFlowFields(topic string, maxRetries int) error {
	if maxRetries < 0 {
		return fmt.Errorf("topic %q: maxRetries must be >= 0", topic)
	}
	return nil
}

func validateSchemaFields(topic, subject string, version int) error {
	if subject == "" && version != 0 {
		return fmt.Errorf("topic %q: SchemaSubject is required when SchemaVersion is set", topic)
	}
	if subject != "" && version <= 0 {
		return fmt.Errorf("topic %q: SchemaVersion must be > 0 when SchemaSubject is set", topic)
	}
	return nil
}

func validateConsumerSet(consumers []TopicConsumer) error {
	type pair struct{ topic, group string }

	seen := make(map[pair]struct{}, len(consumers))
	for _, c := range consumers {
		p := pair{topic: c.Topic, group: c.ConsumerGroup}
		if _, dup := seen[p]; dup {
			return fmt.Errorf("kafkaapp: duplicate consumer for topic %q group %q", c.Topic, c.ConsumerGroup)
		}
		seen[p] = struct{}{}
	}

	return nil
}
