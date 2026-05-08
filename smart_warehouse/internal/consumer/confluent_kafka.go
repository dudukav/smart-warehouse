package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaConfig struct {
	BootstrapServers string
	GroupID          string
	Topic            string
}

type ConfluentKafkaClient struct {
	consumer *kafka.Consumer
	topic    string
}

func NewConfluentKafkaClient(cfg KafkaConfig) (*ConfluentKafkaClient, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"group.id":           cfg.GroupID,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	if err := consumer.SubscribeTopics([]string{cfg.Topic}, nil); err != nil {
		_ = consumer.Close()
		return nil, fmt.Errorf("subscribe to kafka topic %q: %w", cfg.Topic, err)
	}

	return &ConfluentKafkaClient{
		consumer: consumer,
		topic:    cfg.Topic,
	}, nil
}

func (c *ConfluentKafkaClient) ReadMessage(ctx context.Context) (*KafkaMessage, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		msg, err := c.consumer.ReadMessage(500 * time.Millisecond)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				continue
			}

			return nil, fmt.Errorf("read kafka message: %w", err)
		}

		return &KafkaMessage{
			Key:       msg.Key,
			Value:     msg.Value,
			Topic:     topicName(msg.TopicPartition.Topic),
			Partition: msg.TopicPartition.Partition,
			Offset:    int64(msg.TopicPartition.Offset),
		}, nil
	}
}

func (c *ConfluentKafkaClient) CommitMessage(ctx context.Context, msg *KafkaMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	topic := msg.Topic
	if topic == "" {
		topic = c.topic
	}

	_, err := c.consumer.CommitOffsets([]kafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: msg.Partition,
			Offset:    kafka.Offset(msg.Offset + 1),
		},
	})
	if err != nil {
		return fmt.Errorf("commit kafka offset topic=%s partition=%d offset=%d: %w", topic, msg.Partition, msg.Offset+1, err)
	}

	return nil
}

func (c *ConfluentKafkaClient) Close() error {
	return c.consumer.Close()
}

func topicName(topic *string) string {
	if topic == nil {
		return ""
	}

	return *topic
}
