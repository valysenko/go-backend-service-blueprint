package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func ReplayDeadLetters(ctx context.Context, cfg Config, spec QueueSpec, limit int) (int, error) {
	conn, err := Dial(cfg)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		return 0, fmt.Errorf("rabbitmq: open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := Declare(ch, spec); err != nil {
		return 0, err
	}
	if err := ch.Confirm(false); err != nil {
		return 0, fmt.Errorf("rabbitmq: enable publisher confirms: %w", err)
	}
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))

	deadLetter := DeadLetterQueueName(spec.Name)
	q, err := ch.QueueDeclarePassive(deadLetter, true, false, false, false, nil)
	if err != nil {
		return 0, fmt.Errorf("rabbitmq: inspect queue %q: %w", deadLetter, err)
	}

	pending := q.Messages
	if limit > 0 {
		pending = min(pending, limit)
	}

	replayed := 0
	for replayed < pending {
		if err := ctx.Err(); err != nil {
			return replayed, err
		}

		d, ok, err := ch.Get(deadLetter, false)
		if err != nil {
			return replayed, fmt.Errorf("rabbitmq: get from %q: %w", deadLetter, err)
		}
		if !ok {
			return replayed, nil
		}

		// The dead letter is the only copy left, so it goes back on the queue
		// unless the replay is confirmed and routed.
		if err := republish(ctx, ch, returns, spec, d); err != nil {
			_ = d.Nack(false, true)
			return replayed, err
		}
		if err := d.Ack(false); err != nil {
			return replayed, fmt.Errorf("rabbitmq: ack %q: %w", deadLetter, err)
		}
		replayed++
	}

	return replayed, nil
}

func republish(ctx context.Context, ch *amqp.Channel, returns <-chan amqp.Return, spec QueueSpec, d amqp.Delivery) error {
	drainReturns(returns)
	// Replay is immediate: routing through the default exchange allows the
	// broker to return an unroutable message even for a delayed queue.
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", spec.Name, true, false, amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Timestamp:    d.Timestamp,
		Headers:      d.Headers,
		Body:         d.Body,
	})
	if err != nil {
		return fmt.Errorf("rabbitmq: replay to %q: %w", spec.Name, err)
	}
	if confirmation == nil {
		return fmt.Errorf("rabbitmq: replay to %q: channel is not in confirm mode", spec.Name)
	}

	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("rabbitmq: replay to %q: wait for confirm: %w", spec.Name, err)
	}
	if !acked {
		return fmt.Errorf("rabbitmq: replay to %q: %w", spec.Name, ErrPublishNacked)
	}
	if err := unroutable(returns); err != nil {
		return fmt.Errorf("rabbitmq: replay to %q: %w", spec.Name, err)
	}

	return nil
}
