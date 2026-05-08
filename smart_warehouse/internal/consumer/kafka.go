package consumer

import (
	"context"

	"smart_warehouse/internal/events"
)

type KafkaMessage struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
}

type KafkaMetadata struct {
	Topic     string
	Partition int32
	Offset    int64
}

type MessageReader interface {
	ReadMessage(ctx context.Context) (*KafkaMessage, error)
}

type OffsetCommitter interface {
	CommitMessage(ctx context.Context, msg *KafkaMessage) error
}

type EventDecoder interface {
	Decode(data []byte) (*events.WarehouseEvent, error)
}

type EventHandler interface {
	Handle(ctx context.Context, event *events.WarehouseEvent, meta KafkaMetadata) error
}

type DLQPublisher interface {
	Publish(ctx context.Context, msg *KafkaMessage, err error) error
}
