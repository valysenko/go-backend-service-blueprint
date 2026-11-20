package rabbitmq

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Kind string

const (
	KindDirect  Kind = "direct"
	KindDelayed Kind = "delayed"
)

type QueueSpec struct {
	Name string
	Kind Kind
}

func (s QueueSpec) validate() error {
	if s.Name == "" {
		return errors.New("rabbitmq: queue name must not be empty")
	}
	if s.Kind != "" && s.Kind != KindDirect && s.Kind != KindDelayed {
		return fmt.Errorf("rabbitmq: unknown queue kind %q", s.Kind)
	}
	return nil
}

// routesOnPublish reports whether the broker picks the destination queue while
// the message is being published. The delayed message exchange instead stores
// the message and routes it once the delay expires, so it answers every
// mandatory publish with an unroutable return even though the message is fine.
// Nothing can be asserted about the routing of a delayed message up front.
func (s QueueSpec) routesOnPublish() bool {
	return s.Kind != KindDelayed
}

func (s QueueSpec) exchange() string {
	if s.Kind == KindDelayed {
		return s.Name + ".delayed"
	}
	return ""
}

func Declare(ch *amqp.Channel, spec QueueSpec) error {
	if err := spec.validate(); err != nil {
		return err
	}
	deadLetter := DeadLetterQueueName(spec.Name)
	if _, err := ch.QueueDeclare(deadLetter, true, false, false, false, nil); err != nil {
		return fmt.Errorf("rabbitmq: declare queue %q: %w", deadLetter, err)
	}
	if _, err := ch.QueueDeclare(spec.Name, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": deadLetter,
	}); err != nil {
		return fmt.Errorf("rabbitmq: declare queue %q: %w", spec.Name, err)
	}
	if spec.Kind == KindDelayed {
		exchange := spec.exchange()
		if err := ch.ExchangeDeclare(exchange, "x-delayed-message", true, false, false, false,
			amqp.Table{"x-delayed-type": "direct"}); err != nil {
			return fmt.Errorf("rabbitmq: declare delayed exchange %q: %w", exchange, err)
		}
		if err := ch.QueueBind(spec.Name, spec.Name, exchange, false, nil); err != nil {
			return fmt.Errorf("rabbitmq: bind queue %q: %w", spec.Name, err)
		}
	}
	return nil
}
