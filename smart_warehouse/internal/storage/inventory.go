package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"smart_warehouse/internal/events"
)

type InventoryState struct {
	ProductSKU         string
	ZoneID             string
	AvailableQuantity  int32
	ReservedQuantity   int32
	LastSequenceNumber int64
	LastEventAt        time.Time
	SupplierID         *string
	UpdatedAt          time.Time
}

func (s *CassandraStore) getInventoryState(
	ctx context.Context,
	productSKU string,
	zoneID string,
) (InventoryState, bool, error) {
	state := InventoryState{
		ProductSKU: productSKU,
		ZoneID:     zoneID,
	}

	err := s.session.Query(`
		SELECT available_quantity,
		       reserved_quantity,
		       last_sequence_number,
		       last_event_at,
		       supplier_id,
		       updated_at
		FROM inventory_by_product_zone
		WHERE product_sku = ? AND zone_id = ?
	`, productSKU, zoneID).WithContext(ctx).Scan(
		&state.AvailableQuantity,
		&state.ReservedQuantity,
		&state.LastSequenceNumber,
		&state.LastEventAt,
		&state.SupplierID,
		&state.UpdatedAt,
	)

	if errors.Is(err, gocql.ErrNotFound) {
		return state, false, nil
	}
	if err != nil {
		return InventoryState{}, false, fmt.Errorf("select inventory state product_sku=%s zone_id=%s: %w", productSKU, zoneID, err)
	}

	return state, true, nil
}

func (s *CassandraStore) addInventoryStateUpdate(batch *gocql.Batch, state InventoryState) {
	batch.Query(`
		UPDATE inventory_by_product_zone
		SET available_quantity = ?,
		    reserved_quantity = ?,
		    last_sequence_number = ?,
		    last_event_at = ?,
		    supplier_id = ?,
		    updated_at = ?
		WHERE product_sku = ? AND zone_id = ?
	`,
		state.AvailableQuantity,
		state.ReservedQuantity,
		state.LastSequenceNumber,
		state.LastEventAt,
		state.SupplierID,
		state.UpdatedAt,
		state.ProductSKU,
		state.ZoneID,
	)

	batch.Query(`
		UPDATE inventory_by_product
		SET available_quantity = ?,
		    reserved_quantity = ?,
		    last_sequence_number = ?,
		    last_event_at = ?,
		    supplier_id = ?,
		    updated_at = ?
		WHERE product_sku = ? AND zone_id = ?
	`,
		state.AvailableQuantity,
		state.ReservedQuantity,
		state.LastSequenceNumber,
		state.LastEventAt,
		state.SupplierID,
		state.UpdatedAt,
		state.ProductSKU,
		state.ZoneID,
	)

	batch.Query(`
		UPDATE inventory_by_zone
		SET available_quantity = ?,
		    reserved_quantity = ?,
		    last_sequence_number = ?,
		    last_event_at = ?,
		    supplier_id = ?,
		    updated_at = ?
		WHERE zone_id = ? AND product_sku = ?
	`,
		state.AvailableQuantity,
		state.ReservedQuantity,
		state.LastSequenceNumber,
		state.LastEventAt,
		state.SupplierID,
		state.UpdatedAt,
		state.ZoneID,
		state.ProductSKU,
	)
}

func isStaleEvent(state InventoryState, event *events.WarehouseEvent) bool {
	return event.SequenceNumber <= state.LastSequenceNumber
}

func updateInventoryMetadata(state *InventoryState, event *events.WarehouseEvent, processedAt time.Time) {
	state.LastSequenceNumber = event.SequenceNumber
	state.LastEventAt = time.UnixMilli(event.OccurredAt)
	state.UpdatedAt = processedAt
}

func ensureAvailable(state InventoryState, quantity int32) error {
	if state.AvailableQuantity < quantity {
		return events.NewValidationError(
			"BUSINESS_RULE_ERROR",
			"available_quantity",
			fmt.Sprintf("insufficient available quantity for product_sku=%s zone_id=%s: requested %d, available %d", state.ProductSKU, state.ZoneID, quantity, state.AvailableQuantity),
		)
	}

	return nil
}

func ensureReserved(state InventoryState, quantity int32) error {
	if state.ReservedQuantity < quantity {
		return events.NewValidationError(
			"BUSINESS_RULE_ERROR",
			"reserved_quantity",
			fmt.Sprintf("insufficient reserved quantity for product_sku=%s zone_id=%s: requested %d, reserved %d", state.ProductSKU, state.ZoneID, quantity, state.ReservedQuantity),
		)
	}

	return nil
}
