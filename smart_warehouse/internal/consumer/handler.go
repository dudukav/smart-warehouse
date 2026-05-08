package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"smart_warehouse/internal/events"
)

type EventStore interface {
	IsProcessed(ctx context.Context, eventID string) (bool, error)
	ApplyProductReceived(ctx context.Context, event *events.WarehouseEvent, meta KafkaMetadata) error
	ApplyProductShipped(ctx context.Context, event *events.WarehouseEvent, meta KafkaMetadata) error
	ApplyProductMoved(ctx context.Context, event *events.WarehouseEvent, meta KafkaMetadata) error
	ApplyProductReserved(ctx context.Context, event *events.WarehouseEvent, meta KafkaMetadata) error
	ApplyProductReleased(ctx context.Context, event *events.WarehouseEvent, meta KafkaMetadata) error
	ApplyInventoryCounted(ctx context.Context, event *events.WarehouseEvent, meta KafkaMetadata) error
	ApplyOrderCreated(ctx context.Context, event *events.WarehouseEvent, meta KafkaMetadata) error
	ApplyOrderCompleted(ctx context.Context, event *events.WarehouseEvent, meta KafkaMetadata) error
}

type Handler struct {
	store  EventStore
	logger *slog.Logger
}

func NewHandler(store EventStore, logger *slog.Logger) *Handler {
	return &Handler{
		store:  store,
		logger: logger,
	}
}

func (h *Handler) Handle(ctx context.Context, event *events.WarehouseEvent, meta KafkaMetadata) error {
	processed, err := h.store.IsProcessed(ctx, event.EventID)
	if err != nil {
		return fmt.Errorf("check processed event %s: %w", event.EventID, err)
	}

	if processed {
		h.logger.Info(
			"duplicate event skipped",
			"event_id", event.EventID,
			"event_type", event.EventType,
			"partition", meta.Partition,
			"offset", meta.Offset,
		)
		return nil
	}

	switch event.EventType {
	case events.EventTypeProductReceived:
		return h.store.ApplyProductReceived(ctx, event, meta)
	case events.EventTypeProductShipped:
		return h.store.ApplyProductShipped(ctx, event, meta)
	case events.EventTypeProductMoved:
		return h.store.ApplyProductMoved(ctx, event, meta)
	case events.EventTypeProductReserved:
		return h.store.ApplyProductReserved(ctx, event, meta)
	case events.EventTypeProductReleased:
		return h.store.ApplyProductReleased(ctx, event, meta)
	case events.EventTypeInventoryCounted:
		return h.store.ApplyInventoryCounted(ctx, event, meta)
	case events.EventTypeOrderCreated:
		return h.store.ApplyOrderCreated(ctx, event, meta)
	case events.EventTypeOrderCompleted:
		return h.store.ApplyOrderCompleted(ctx, event, meta)
	default:
		return events.NewValidationError("UNKNOWN_EVENT_TYPE", "event_type", fmt.Sprintf("unsupported event type %q", event.EventType))
	}
}
