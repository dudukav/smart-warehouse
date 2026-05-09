package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"smart_warehouse/internal/consumer"
	"smart_warehouse/internal/events"
)

const (
	orderStatusCreated   = "CREATED"
	orderStatusCompleted = "COMPLETED"
)

func (s *CassandraStore) ApplyProductReceived(ctx context.Context, event *events.WarehouseEvent, meta consumer.KafkaMetadata) error {
	payload := event.ProductReceived
	processedAt := time.Now().UTC()

	state, _, err := s.getInventoryState(ctx, payload.ProductSKU, payload.ZoneID)
	if err != nil {
		return err
	}
	if isStaleEvent(state, event) {
		return s.recordEventWithoutStateChange(ctx, event, meta, processedAt)
	}

	state.AvailableQuantity += payload.Quantity
	if payload.SupplierID != nil {
		state.SupplierID = payload.SupplierID
	}
	updateInventoryMetadata(&state, event, processedAt)

	return s.executeEventBatch(ctx, event, meta, processedAt, func(batch *gocql.Batch) error {
		s.addInventoryStateUpdate(batch, state)
		return nil
	})
}

func (s *CassandraStore) ApplyProductShipped(ctx context.Context, event *events.WarehouseEvent, meta consumer.KafkaMetadata) error {
	payload := event.ProductShipped
	processedAt := time.Now().UTC()

	state, _, err := s.getInventoryState(ctx, payload.ProductSKU, payload.ZoneID)
	if err != nil {
		return err
	}
	if isStaleEvent(state, event) {
		return s.recordEventWithoutStateChange(ctx, event, meta, processedAt)
	}
	if err := ensureAvailable(state, payload.Quantity); err != nil {
		return err
	}

	state.AvailableQuantity -= payload.Quantity
	updateInventoryMetadata(&state, event, processedAt)

	return s.executeEventBatch(ctx, event, meta, processedAt, func(batch *gocql.Batch) error {
		s.addInventoryStateUpdate(batch, state)
		return nil
	})
}

func (s *CassandraStore) ApplyProductMoved(ctx context.Context, event *events.WarehouseEvent, meta consumer.KafkaMetadata) error {
	payload := event.ProductMoved
	processedAt := time.Now().UTC()

	fromState, _, err := s.getInventoryState(ctx, payload.ProductSKU, payload.FromZoneID)
	if err != nil {
		return err
	}
	toState, _, err := s.getInventoryState(ctx, payload.ProductSKU, payload.ToZoneID)
	if err != nil {
		return err
	}
	if isStaleEvent(fromState, event) || isStaleEvent(toState, event) {
		return s.recordEventWithoutStateChange(ctx, event, meta, processedAt)
	}
	if err := ensureAvailable(fromState, payload.Quantity); err != nil {
		return err
	}

	fromState.AvailableQuantity -= payload.Quantity
	toState.AvailableQuantity += payload.Quantity
	updateInventoryMetadata(&fromState, event, processedAt)
	updateInventoryMetadata(&toState, event, processedAt)

	return s.executeEventBatch(ctx, event, meta, processedAt, func(batch *gocql.Batch) error {
		s.addInventoryStateUpdate(batch, fromState)
		s.addInventoryStateUpdate(batch, toState)
		return nil
	})
}

func (s *CassandraStore) ApplyProductReserved(ctx context.Context, event *events.WarehouseEvent, meta consumer.KafkaMetadata) error {
	payload := event.ProductReserved
	processedAt := time.Now().UTC()

	state, _, err := s.getInventoryState(ctx, payload.ProductSKU, payload.ZoneID)
	if err != nil {
		return err
	}
	if isStaleEvent(state, event) {
		return s.recordEventWithoutStateChange(ctx, event, meta, processedAt)
	}
	if err := ensureAvailable(state, payload.Quantity); err != nil {
		return err
	}

	state.AvailableQuantity -= payload.Quantity
	state.ReservedQuantity += payload.Quantity
	updateInventoryMetadata(&state, event, processedAt)

	return s.executeEventBatch(ctx, event, meta, processedAt, func(batch *gocql.Batch) error {
		s.addInventoryStateUpdate(batch, state)
		return nil
	})
}

func (s *CassandraStore) ApplyProductReleased(ctx context.Context, event *events.WarehouseEvent, meta consumer.KafkaMetadata) error {
	payload := event.ProductReleased
	processedAt := time.Now().UTC()

	state, _, err := s.getInventoryState(ctx, payload.ProductSKU, payload.ZoneID)
	if err != nil {
		return err
	}
	if isStaleEvent(state, event) {
		return s.recordEventWithoutStateChange(ctx, event, meta, processedAt)
	}
	if err := ensureReserved(state, payload.Quantity); err != nil {
		return err
	}

	state.ReservedQuantity -= payload.Quantity
	state.AvailableQuantity += payload.Quantity
	updateInventoryMetadata(&state, event, processedAt)

	return s.executeEventBatch(ctx, event, meta, processedAt, func(batch *gocql.Batch) error {
		s.addInventoryStateUpdate(batch, state)
		return nil
	})
}

func (s *CassandraStore) ApplyInventoryCounted(ctx context.Context, event *events.WarehouseEvent, meta consumer.KafkaMetadata) error {
	payload := event.InventoryCounted
	processedAt := time.Now().UTC()

	state, _, err := s.getInventoryState(ctx, payload.ProductSKU, payload.ZoneID)
	if err != nil {
		return err
	}
	if isStaleEvent(state, event) {
		return s.recordEventWithoutStateChange(ctx, event, meta, processedAt)
	}

	state.AvailableQuantity = payload.ActualQuantity
	updateInventoryMetadata(&state, event, processedAt)

	return s.executeEventBatch(ctx, event, meta, processedAt, func(batch *gocql.Batch) error {
		s.addInventoryStateUpdate(batch, state)
		return nil
	})
}

func (s *CassandraStore) ApplyOrderCreated(ctx context.Context, event *events.WarehouseEvent, meta consumer.KafkaMetadata) error {
	payload := event.OrderCreated
	processedAt := time.Now().UTC()
	createdAt := time.UnixMilli(event.OccurredAt)

	states := make([]InventoryState, 0, len(payload.Items))
	for _, item := range payload.Items {
		state, _, err := s.getInventoryState(ctx, item.ProductSKU, item.ZoneID)
		if err != nil {
			return err
		}
		if isStaleEvent(state, event) {
			return s.recordEventWithoutStateChange(ctx, event, meta, processedAt)
		}
		if err := ensureAvailable(state, item.Quantity); err != nil {
			return err
		}

		state.AvailableQuantity -= item.Quantity
		state.ReservedQuantity += item.Quantity
		updateInventoryMetadata(&state, event, processedAt)
		states = append(states, state)
	}

	return s.executeEventBatch(ctx, event, meta, processedAt, func(batch *gocql.Batch) error {
		batch.Query(`
			INSERT INTO orders_by_id (
				order_id,
				status,
				created_at,
				completed_at,
				last_sequence_number,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?)
		`,
			payload.OrderID,
			orderStatusCreated,
			createdAt,
			nil,
			event.SequenceNumber,
			processedAt,
		)

		for _, item := range payload.Items {
			batch.Query(`
				INSERT INTO order_items_by_order (
					order_id,
					product_sku,
					zone_id,
					quantity
				) VALUES (?, ?, ?, ?)
			`,
				payload.OrderID,
				item.ProductSKU,
				item.ZoneID,
				item.Quantity,
			)
		}

		for _, state := range states {
			s.addInventoryStateUpdate(batch, state)
		}

		return nil
	})
}

func (s *CassandraStore) ApplyOrderCompleted(ctx context.Context, event *events.WarehouseEvent, meta consumer.KafkaMetadata) error {
	payload := event.OrderCompleted
	processedAt := time.Now().UTC()
	completedAt := time.UnixMilli(event.OccurredAt)

	items, err := s.getOrderItems(ctx, payload.OrderID)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return events.NewValidationError("BUSINESS_RULE_ERROR", "order_id", fmt.Sprintf("order %s has no items", payload.OrderID))
	}

	states := make([]InventoryState, 0, len(items))
	for _, item := range items {
		state, _, err := s.getInventoryState(ctx, item.ProductSKU, item.ZoneID)
		if err != nil {
			return err
		}
		if isStaleEvent(state, event) {
			return s.recordEventWithoutStateChange(ctx, event, meta, processedAt)
		}
		if err := ensureReserved(state, item.Quantity); err != nil {
			return err
		}

		state.ReservedQuantity -= item.Quantity
		updateInventoryMetadata(&state, event, processedAt)
		states = append(states, state)
	}

	return s.executeEventBatch(ctx, event, meta, processedAt, func(batch *gocql.Batch) error {
		batch.Query(`
			UPDATE orders_by_id
			SET status = ?,
			    completed_at = ?,
			    last_sequence_number = ?,
			    updated_at = ?
			WHERE order_id = ?
		`,
			orderStatusCompleted,
			completedAt,
			event.SequenceNumber,
			processedAt,
			payload.OrderID,
		)

		for _, state := range states {
			s.addInventoryStateUpdate(batch, state)
		}

		return nil
	})
}

func (s *CassandraStore) executeEventBatch(
	ctx context.Context,
	event *events.WarehouseEvent,
	meta consumer.KafkaMetadata,
	processedAt time.Time,
	addStateChanges func(batch *gocql.Batch) error,
) error {
	batch := s.newLoggedBatch(ctx)

	if err := addStateChanges(batch); err != nil {
		return err
	}
	s.addProcessedEvent(batch, event, meta, processedAt)
	if err := s.addEventHistory(batch, event, meta, processedAt); err != nil {
		return err
	}

	if err := s.session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("execute event batch event_id=%s event_type=%s: %w", event.EventID, event.EventType, err)
	}

	return nil
}

func (s *CassandraStore) recordEventWithoutStateChange(
	ctx context.Context,
	event *events.WarehouseEvent,
	meta consumer.KafkaMetadata,
	processedAt time.Time,
) error {
	return s.executeEventBatch(ctx, event, meta, processedAt, func(batch *gocql.Batch) error {
		return nil
	})
}
