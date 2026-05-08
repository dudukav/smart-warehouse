package consumer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"smart_warehouse/internal/events"
)

type DLQConfig struct {
	BootstrapServers string
	Topic            string
}

type KafkaDLQPublisher struct {
	producer *kafka.Producer
	topic    string
}

type DLQMessage struct {
	OriginalEventBase64 string           `json:"original_event_base64"`
	OriginalKeyBase64   string           `json:"original_key_base64,omitempty"`
	ErrorCode           string           `json:"error_code"`
	ErrorReason         string           `json:"error_reason"`
	Field               string           `json:"field,omitempty"`
	FailedAt            time.Time        `json:"failed_at"`
	KafkaMetadata       DLQKafkaMetadata `json:"kafka_metadata"`
}

type DLQKafkaMetadata struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func NewKafkaDLQPublisher(cfg DLQConfig) (*KafkaDLQPublisher, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
	})
	if err != nil {
		return nil, fmt.Errorf("create dlq kafka producer: %w", err)
	}

	return &KafkaDLQPublisher{
		producer: producer,
		topic:    cfg.Topic,
	}, nil
}

func (p *KafkaDLQPublisher) Publish(ctx context.Context, msg *KafkaMessage, cause error) error {
	payload, err := json.Marshal(newDLQMessage(msg, cause))
	if err != nil {
		return fmt.Errorf("marshal dlq message: %w", err)
	}

	delivery := make(chan kafka.Event, 1)
	defer close(delivery)

	if err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: kafka.PartitionAny,
		},
		Key:   msg.Key,
		Value: payload,
	}, delivery); err != nil {
		return fmt.Errorf("produce dlq message: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case event := <-delivery:
		deliveredMessage, ok := event.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected dlq delivery event: %T", event)
		}
		if deliveredMessage.TopicPartition.Error != nil {
			return fmt.Errorf("deliver dlq message: %w", deliveredMessage.TopicPartition.Error)
		}

		return nil
	}
}

func (p *KafkaDLQPublisher) Close() {
	p.producer.Close()
}

func newDLQMessage(msg *KafkaMessage, cause error) DLQMessage {
	errorCode := "PROCESSING_ERROR"
	errorReason := cause.Error()
	field := ""

	var validationErr *events.ValidationError
	if errors.As(cause, &validationErr) {
		errorCode = validationErr.Code
		errorReason = validationErr.Message
		field = validationErr.Field
	}

	dlqMessage := DLQMessage{
		OriginalEventBase64: base64.StdEncoding.EncodeToString(msg.Value),
		ErrorCode:           errorCode,
		ErrorReason:         errorReason,
		Field:               field,
		FailedAt:            time.Now().UTC(),
		KafkaMetadata: DLQKafkaMetadata{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		},
	}

	if len(msg.Key) > 0 {
		dlqMessage.OriginalKeyBase64 = base64.StdEncoding.EncodeToString(msg.Key)
	}

	return dlqMessage
}
