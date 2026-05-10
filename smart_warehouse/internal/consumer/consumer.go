package consumer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"smart_warehouse/internal/events"
	"smart_warehouse/internal/metrics"
)

type Consumer struct {
	reader    MessageReader
	committer OffsetCommitter
	decoder   EventDecoder
	handler   EventHandler
	dlq       DLQPublisher
	metrics   *metrics.Metrics
	logger    *slog.Logger
}

func New(
	reader MessageReader,
	committer OffsetCommitter,
	decoder EventDecoder,
	handler EventHandler,
	dlq DLQPublisher,
	appMetrics *metrics.Metrics,
	logger *slog.Logger,
) *Consumer {
	return &Consumer{
		reader:    reader,
		committer: committer,
		decoder:   decoder,
		handler:   handler,
		dlq:       dlq,
		metrics:   appMetrics,
		logger:    logger,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("consumer started")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("consumer stopped", "reason", ctx.Err())
			return ctx.Err()
		default:
		}

		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			c.logger.Error("failed to read kafka message", "error", err)
			continue
		}

		if err := c.processMessage(ctx, msg); err != nil {
			c.logger.Error(
				"failed to process kafka message",
				"error", err,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *KafkaMessage) error {
	startedAt := time.Now()
	eventType := "unknown"
	defer func() {
		c.metrics.ObserveProcessingDuration(eventType, time.Since(startedAt))
	}()

	c.metrics.SetConsumerLag(msg.Topic, msg.Partition, msg.Lag)

	event, err := c.decoder.Decode(msg.Value)
	if err != nil {
		return c.publishToDLQAndCommit(ctx, msg, err)
	}
	eventType = event.EventType

	meta := KafkaMetadata{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}

	c.logger.Info(
		"kafka event received",
		"event_id", event.EventID,
		"event_type", event.EventType,
		"partition", meta.Partition,
		"offset", meta.Offset,
	)

	if err := event.Validate(); err != nil {
		return c.publishToDLQAndCommit(ctx, msg, err)
	}

	if err := c.handler.Handle(ctx, event, meta); err != nil {
		var validationErr *events.ValidationError
		if !errors.As(err, &validationErr) {
			c.metrics.CassandraWriteError()
		}
		return c.publishToDLQAndCommit(ctx, msg, err)
	}

	c.metrics.EventProcessed(event.EventType)

	if err := c.committer.CommitMessage(ctx, msg); err != nil {
		return err
	}

	c.logger.Info(
		"kafka event processed",
		"event_id", event.EventID,
		"event_type", event.EventType,
		"partition", meta.Partition,
		"offset", meta.Offset,
	)

	return nil
}

func (c *Consumer) publishToDLQAndCommit(ctx context.Context, msg *KafkaMessage, err error) error {
	var validationErr *events.ValidationError
	if !errors.As(err, &validationErr) {
		return err
	}

	if publishErr := c.dlq.Publish(ctx, msg, validationErr); publishErr != nil {
		return publishErr
	}

	c.logger.Warn(
		"event sent to dlq",
		"error_code", validationErr.Code,
		"error_reason", validationErr.Message,
		"field", validationErr.Field,
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)

	return c.committer.CommitMessage(ctx, msg)
}
