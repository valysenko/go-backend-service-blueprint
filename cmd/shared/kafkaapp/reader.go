package kafkaapp

import (
	"fmt"
	"io"

	pkgConfig "go-backend-service-blueprint/pkg/kafka/config"
	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/kafka/reader/handler"
	"go-backend-service-blueprint/pkg/kafka/storage"
	"go-backend-service-blueprint/pkg/kafka/writer"
)

// kafka consumers: reader->handler(chain)->processor(domain logic)

func (a *App) newTopicReader(c TopicConsumer) (*reader.KafkaTopicReader, error) {
	cfg := &pkgConfig.KafkaConfig{
		Brokers: a.opts.Brokers,
	}

	h, closers, err := a.newHandler(cfg, c)
	if err != nil {
		return nil, err
	}

	client, err := reader.NewKafkaClient(cfg, a.logger, c.Topic, c.ConsumerGroup)
	if err != nil {
		for _, cl := range closers {
			if cerr := cl.Close(); cerr != nil {
				a.logger.Error("close on reader init failure", "error", cerr)
			}
		}
		return nil, err
	}

	return reader.NewKafkaTopicReader(client, h, a.logger, closers...), nil
}

func (a *App) newHandler(cfg *pkgConfig.KafkaConfig, c TopicConsumer) (reader.MessageHandler, []io.Closer, error) {
	dedupStorage := storage.NewDeduplicationStorage(a.opts.Redis, c.ConsumerGroup)
	dlStorage := storage.NewDlStorage(a.opts.Redis, c.DLSource)

	// collect handlers resources which should be closed on kafka reader shutdown. f.e:
	// - dl handlers have kafka writers
	var closers []io.Closer

	switch c.Mode() {
	case BaseFlow:
		// DL writer should NOT validate messages
		w, err := writer.NewWriter(*cfg, c.DLTopic, a.logger)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create DL writer for topic %q: %w", c.DLTopic, err)
		}
		closers = append(closers, w) // there is a dl topic writer for the base flow
		return handler.NewBaseFlowHandler(
			c.Processor, c.RetryConfig,
			dedupStorage, dlStorage, w,
			a.logger, a.opts.Metrics, c.DLTopic,
			a.opts.Validator, c.SchemaSubject, c.SchemaVersion,
		), closers, nil

	case DLFlow:
		return handler.NewDLFlowHandler(
			c.Processor,
			dedupStorage, dlStorage,
			a.logger, a.opts.Metrics,
			c.DLMaxRetries,
			a.opts.Validator, c.SchemaSubject, c.SchemaVersion,
		), closers, nil

	default:
		return nil, nil, fmt.Errorf("unknown flow mode %d for topic %q", c.Mode(), c.Topic)
	}
}
