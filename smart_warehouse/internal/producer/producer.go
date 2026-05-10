package producer

import (
	"context"
	"fmt"
	"log/slog"
	"smart_warehouse/internal/events"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type EventEncoder interface {
	Encode(event *events.WarehouseEvent) ([]byte, error)
}

type Producer struct {
	producer 	*kafka.Producer
	encoder 	EventEncoder
	topic 		string
	logger 		*slog.Logger
}

type Config struct {
	BootstrapServers string
	Topic string
}

func New(
	cfg Config,
	encoder EventEncoder,
	logger *slog.Logger,
) (*Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  	cfg.BootstrapServers,
		"acks":              	"all",
	})

	if err != nil {
		return nil, fmt.Errorf("create new kafka producer: %w", err)
	}

	return &Producer{
		producer: producer,
		encoder: encoder,
		topic: cfg.Topic,
		logger: logger,
	}, nil
}

func (p *Producer) Publish(ctx context.Context, event events.WarehouseEvent) error {
	value, err := p.encoder.Encode(&event)
	if err != nil {
		return err
	}

	key := events.PartitionKey(event)

	delivery := make(chan kafka.Event, 1)
	defer close(delivery)

	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &p.topic,
			Partition: kafka.PartitionAny,
		},
		Value: value,
		Key: []byte(key),
	}, delivery)

	if err != nil {
		return fmt.Errorf("produce kafka event %s: %w", event.EventID, err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case deliveryEvent := <-delivery:
		msg, ok := deliveryEvent.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected delivery event: %T", deliveryEvent)
		}

		if msg.TopicPartition.Error != nil {
			return fmt.Errorf("deliver event %s: %w", event.EventID, msg.TopicPartition.Error)
		}

		p.logger.Info(
			"event published",
			"event_id", event.EventID,
			"event_type", event.EventType,
			"schema_version", event.SchemaVersion,
			"topic", msg.TopicPartition.Topic,
			"partition", msg.TopicPartition.Partition,
			"offset", msg.TopicPartition.Offset,
			"key", key,
		)

		return nil
	}
}

func (p *Producer) Close() {
	p.producer.Flush(5000)
	p.producer.Close()
}
