package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"smart_warehouse/internal/consumer"
	"smart_warehouse/internal/events"
)

func (s *CassandraStore) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	var existingID string

	err := s.session.Query(`
		SELECT event_id
		FROM processed_events
		WHERE event_id = ?
	`, eventID).WithContext(ctx).Scan(&existingID)

	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("select processed event: %w", err)
	}

	return true, nil
}

func (s *CassandraStore) addProcessedEvent(
	batch *gocql.Batch,
	event *events.WarehouseEvent,
	meta consumer.KafkaMetadata,
	processedAt time.Time,
) {
	batch.Query(`
		INSERT INTO processed_events (
			event_id,
			event_type,
			processed_at,
			kafka_partition,
			kafka_offset
		) VALUES (?, ?, ?, ?, ?)
	`,
		event.EventID,
		event.EventType,
		processedAt,
		meta.Partition,
		meta.Offset,
	)
}

func (s *CassandraStore) addEventHistory(
	batch *gocql.Batch,
	event *events.WarehouseEvent,
	meta consumer.KafkaMetadata,
	processedAt time.Time,
) error {
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("add event history: %w", err)
	}

	productSKU, zoneID, orderID := eventSummary(event)

	batch.Query(`
		INSERT INTO event_history (
			event_id,
			occurred_at,
			event_type,
			schema_version,
			sequence_number,
			product_sku,
			zone_id,
			order_id,
			kafka_partition,
			kafka_offset,
			payload,
			processed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		event.EventID,
		time.UnixMilli(event.OccurredAt),
		event.EventType,
		event.SchemaVersion,
		event.SequenceNumber,
		productSKU,
		zoneID,
		orderID,
		meta.Partition,
		meta.Offset,
		string(payloadBytes),
		processedAt,
	)

	return nil
}

func eventSummary(event *events.WarehouseEvent) (string, string, string) {
	switch event.EventType {
	case events.EventTypeProductReceived:
		p := event.ProductReceived
		return p.ProductSKU, p.ZoneID, ""

	case events.EventTypeProductShipped:
		p := event.ProductShipped
		orderID := ""
		if p.OrderID != nil {
			orderID = *p.OrderID
		}
		return p.ProductSKU, p.ZoneID, orderID

	case events.EventTypeProductMoved:
		p := event.ProductMoved
		return p.ProductSKU, p.FromZoneID + "->" + p.ToZoneID, ""

	case events.EventTypeProductReserved:
		p := event.ProductReserved
		return p.ProductSKU, p.ZoneID, p.OrderID

	case events.EventTypeProductReleased:
		p := event.ProductReleased
		return p.ProductSKU, p.ZoneID, p.OrderID

	case events.EventTypeInventoryCounted:
		p := event.InventoryCounted
		return p.ProductSKU, p.ZoneID, ""

	case events.EventTypeOrderCreated:
		return "", "", event.OrderCreated.OrderID

	case events.EventTypeOrderCompleted:
		return "", "", event.OrderCompleted.OrderID

	default:
		return "", "", ""
	}
}
