package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"golang.org/x/sync/errgroup"
)

type ConsumerSet struct {
	consumers []*Consumer
}

// NewConsumerSet creates one consumer per queue.
// Each consumer owns its connection and runs its configured number of workers.
func NewConsumerSet(cfg Config, configs []ConsumerConfig, observer Observer, logger *slog.Logger) (*ConsumerSet, error) {
	if len(configs) == 0 || logger == nil {
		return nil, errors.New("rabbitmq: consumer set needs configurations and a logger")
	}
	set := &ConsumerSet{}

	seen := make(map[string]bool)
	for _, config := range configs {
		name := config.Spec.Name
		if seen[name] {
			return nil, fmt.Errorf("rabbitmq: duplicate queue %q", name)
		}
		consumer, err := NewConsumer(cfg, config, observer, logger)
		if err != nil {
			return nil, err
		}
		seen[name] = true
		set.consumers = append(set.consumers, consumer)
	}
	return set, nil
}

func (s *ConsumerSet) Run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	for _, consumer := range s.consumers {
		group.Go(func() error { return consumer.Run(ctx) })
	}
	return group.Wait()
}

func (s *ConsumerSet) Consumers() []*Consumer {
	return append([]*Consumer(nil), s.consumers...)
}
