package events

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

func (e *WarehouseEvent) Validate() error {
	if err := validateRequiredUUID("event_id", e.EventID); err != nil {
		return err
	}
	if e.EventType == "" {
		return NewValidationError("MISSING_FIELD", "event_type", "event_type is required")
	}
	if e.SchemaVersion != 1 && e.SchemaVersion != 2 {
		return NewValidationError("UNSUPPORTED_SCHEMA_VERSION", "schema_version", "schema_version must be 1 or 2")
	}
	if e.SequenceNumber <= 0 {
		return NewValidationError("INVALID_SEQUENCE_NUMBER", "sequence_number", "sequence_number must be positive")
	}
	if e.OccurredAt <= 0 {
		return NewValidationError("INVALID_OCCURRED_AT", "occurred_at", "occurred_at must be a positive unix timestamp in milliseconds")
	}

	if err := e.validateEventType(); err != nil {
		return err
	}

	return e.validatePayload()
}

func (e *WarehouseEvent) validateEventType() error {
	payloadCount := e.payloadCount()
	if payloadCount == 0 {
		return NewValidationError("MISSING_PAYLOAD", "payload", "exactly one event payload is required")
	}
	if payloadCount > 1 {
		return NewValidationError("INVALID_PAYLOAD", "payload", "only one event payload can be set")
	}

	var expectedPayloadSet bool
	var expectedPayloadName string

	switch e.EventType {
	case EventTypeProductReceived:
		expectedPayloadSet = e.ProductReceived != nil
		expectedPayloadName = "product_received"
	case EventTypeProductShipped:
		expectedPayloadSet = e.ProductShipped != nil
		expectedPayloadName = "product_shipped"
	case EventTypeProductMoved:
		expectedPayloadSet = e.ProductMoved != nil
		expectedPayloadName = "product_moved"
	case EventTypeProductReserved:
		expectedPayloadSet = e.ProductReserved != nil
		expectedPayloadName = "product_reserved"
	case EventTypeProductReleased:
		expectedPayloadSet = e.ProductReleased != nil
		expectedPayloadName = "product_released"
	case EventTypeInventoryCounted:
		expectedPayloadSet = e.InventoryCounted != nil
		expectedPayloadName = "inventory_counted"
	case EventTypeOrderCreated:
		expectedPayloadSet = e.OrderCreated != nil
		expectedPayloadName = "order_created"
	case EventTypeOrderCompleted:
		expectedPayloadSet = e.OrderCompleted != nil
		expectedPayloadName = "order_completed"
	default:
		return NewValidationError("UNKNOWN_EVENT_TYPE", "event_type", fmt.Sprintf("unsupported event type %q", e.EventType))
	}

	if !expectedPayloadSet {
		actualPayloadName := e.setPayloadName()
		return NewValidationError(
			"INVALID_PAYLOAD",
			"payload",
			fmt.Sprintf("event_type %s requires %s payload, got %s", e.EventType, expectedPayloadName, actualPayloadName),
		)
	}

	return nil
}

func (e *WarehouseEvent) payloadCount() int {
	count := 0
	if e.ProductReceived != nil {
		count++
	}
	if e.ProductShipped != nil {
		count++
	}
	if e.ProductMoved != nil {
		count++
	}
	if e.ProductReserved != nil {
		count++
	}
	if e.ProductReleased != nil {
		count++
	}
	if e.InventoryCounted != nil {
		count++
	}
	if e.OrderCreated != nil {
		count++
	}
	if e.OrderCompleted != nil {
		count++
	}

	return count
}

func (e *WarehouseEvent) setPayloadName() string {
	switch {
	case e.ProductReceived != nil:
		return "product_received"
	case e.ProductShipped != nil:
		return "product_shipped"
	case e.ProductMoved != nil:
		return "product_moved"
	case e.ProductReserved != nil:
		return "product_reserved"
	case e.ProductReleased != nil:
		return "product_released"
	case e.InventoryCounted != nil:
		return "inventory_counted"
	case e.OrderCreated != nil:
		return "order_created"
	case e.OrderCompleted != nil:
		return "order_completed"
	default:
		return "none"
	}
}

func (e *WarehouseEvent) validatePayload() error {
	switch e.EventType {
	case EventTypeProductReceived:
		return validateProductReceived(e.ProductReceived)
	case EventTypeProductShipped:
		return validateProductShipped(e.ProductShipped)
	case EventTypeProductMoved:
		return validateProductMoved(e.ProductMoved)
	case EventTypeProductReserved:
		return validateProductReserved(e.ProductReserved)
	case EventTypeProductReleased:
		return validateProductReleased(e.ProductReleased)
	case EventTypeInventoryCounted:
		return validateInventoryCounted(e.InventoryCounted)
	case EventTypeOrderCreated:
		return validateOrderCreated(e.OrderCreated)
	case EventTypeOrderCompleted:
		return validateOrderCompleted(e.OrderCompleted)
	default:
		return NewValidationError("UNKNOWN_EVENT_TYPE", "event_type", fmt.Sprintf("unsupported event type %q", e.EventType))
	}
}

func validateProductReceived(payload *ProductReceived) error {
	if err := validateRequiredString("product_received.product_sku", payload.ProductSKU); err != nil {
		return err
	}
	if err := validateRequiredString("product_received.zone_id", payload.ZoneID); err != nil {
		return err
	}
	if err := validatePositiveQuantity("product_received.quantity", payload.Quantity); err != nil {
		return err
	}
	if payload.SupplierID != nil && strings.TrimSpace(*payload.SupplierID) == "" {
		return NewValidationError("INVALID_FIELD", "product_received.supplier_id", "supplier_id must not be blank when provided")
	}

	return nil
}

func validateProductShipped(payload *ProductShipped) error {
	if err := validateRequiredString("product_shipped.product_sku", payload.ProductSKU); err != nil {
		return err
	}
	if err := validateRequiredString("product_shipped.zone_id", payload.ZoneID); err != nil {
		return err
	}
	if err := validatePositiveQuantity("product_shipped.quantity", payload.Quantity); err != nil {
		return err
	}
	if payload.OrderID != nil {
		if err := validateRequiredUUID("product_shipped.order_id", *payload.OrderID); err != nil {
			return err
		}
	}

	return nil
}

func validateProductMoved(payload *ProductMoved) error {
	if err := validateRequiredString("product_moved.product_sku", payload.ProductSKU); err != nil {
		return err
	}
	if err := validateRequiredString("product_moved.from_zone_id", payload.FromZoneID); err != nil {
		return err
	}
	if err := validateRequiredString("product_moved.to_zone_id", payload.ToZoneID); err != nil {
		return err
	}
	if strings.TrimSpace(payload.FromZoneID) == strings.TrimSpace(payload.ToZoneID) {
		return NewValidationError("INVALID_FIELD", "product_moved.to_zone_id", "to_zone_id must be different from from_zone_id")
	}
	if err := validatePositiveQuantity("product_moved.quantity", payload.Quantity); err != nil {
		return err
	}

	return nil
}

func validateProductReserved(payload *ProductReserved) error {
	if err := validateRequiredString("product_reserved.product_sku", payload.ProductSKU); err != nil {
		return err
	}
	if err := validateRequiredUUID("product_reserved.order_id", payload.OrderID); err != nil {
		return err
	}
	if err := validateRequiredString("product_reserved.zone_id", payload.ZoneID); err != nil {
		return err
	}
	if err := validatePositiveQuantity("product_reserved.quantity", payload.Quantity); err != nil {
		return err
	}

	return nil
}

func validateProductReleased(payload *ProductReleased) error {
	if err := validateRequiredString("product_released.product_sku", payload.ProductSKU); err != nil {
		return err
	}
	if err := validateRequiredUUID("product_released.order_id", payload.OrderID); err != nil {
		return err
	}
	if err := validateRequiredString("product_released.zone_id", payload.ZoneID); err != nil {
		return err
	}
	if err := validatePositiveQuantity("product_released.quantity", payload.Quantity); err != nil {
		return err
	}

	return nil
}

func validateInventoryCounted(payload *InventoryCounted) error {
	if err := validateRequiredString("inventory_counted.product_sku", payload.ProductSKU); err != nil {
		return err
	}
	if err := validateRequiredString("inventory_counted.zone_id", payload.ZoneID); err != nil {
		return err
	}
	if payload.ActualQuantity < 0 {
		return NewValidationError("INVALID_QUANTITY", "inventory_counted.actual_quantity", "actual_quantity must be zero or positive")
	}

	return nil
}

func validateOrderCreated(payload *OrderCreated) error {
	if err := validateRequiredUUID("order_created.order_id", payload.OrderID); err != nil {
		return err
	}
	if len(payload.Items) == 0 {
		return NewValidationError("MISSING_FIELD", "order_created.items", "order must contain at least one item")
	}

	seenItems := make(map[string]struct{}, len(payload.Items))
	for i, item := range payload.Items {
		fieldPrefix := fmt.Sprintf("order_created.items[%d]", i)
		if err := validateRequiredString(fieldPrefix+".product_sku", item.ProductSKU); err != nil {
			return err
		}
		if err := validateRequiredString(fieldPrefix+".zone_id", item.ZoneID); err != nil {
			return err
		}
		if err := validatePositiveQuantity(fieldPrefix+".quantity", item.Quantity); err != nil {
			return err
		}

		itemKey := strings.TrimSpace(item.ProductSKU) + "|" + strings.TrimSpace(item.ZoneID)
		if _, ok := seenItems[itemKey]; ok {
			return NewValidationError("DUPLICATE_ORDER_ITEM", fieldPrefix, "order contains duplicate product_sku and zone_id item")
		}
		seenItems[itemKey] = struct{}{}
	}

	return nil
}

func validateOrderCompleted(payload *OrderCompleted) error {
	return validateRequiredUUID("order_completed.order_id", payload.OrderID)
}

func validateRequiredString(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return NewValidationError("MISSING_FIELD", field, "field is required")
	}

	return nil
}

func validateRequiredUUID(field, value string) error {
	if err := validateRequiredString(field, value); err != nil {
		return err
	}
	if _, err := uuid.Parse(value); err != nil {
		return NewValidationError("INVALID_UUID", field, "field must be UUID")
	}

	return nil
}

func validatePositiveQuantity(field string, value int32) error {
	if value <= 0 {
		return NewValidationError("INVALID_QUANTITY", field, "quantity must be positive")
	}

	return nil
}
