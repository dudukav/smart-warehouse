package events

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type SchemaVersion int

const (
	SchemaVersionV1 SchemaVersion = 1
	SchemaVersionV2 SchemaVersion = 2
)

func NewProductReceived(
	version SchemaVersion,
	productSKU string,
	zoneID string,
	quantity int32,
	sequenceNumber int64,
	supplierID *string,
) WarehouseEvent {
	event := newBaseEvent(version, EventTypeProductReceived, sequenceNumber)
	event.ProductReceived = &ProductReceived{
		ProductSKU: productSKU,
		ZoneID:     zoneID,
		Quantity:   quantity,
	}

	if version == SchemaVersionV2 {
		event.ProductReceived.SupplierID = supplierID
	}

	return event
}

func NewProductShipped(
	version SchemaVersion,
	productSKU string,
	zoneID string,
	quantity int32,
	orderID *string,
	sequenceNumber int64,
) WarehouseEvent {
	event := newBaseEvent(version, EventTypeProductShipped, sequenceNumber)
	event.ProductShipped = &ProductShipped{
		ProductSKU: productSKU,
		ZoneID:     zoneID,
		Quantity:   quantity,
		OrderID:    orderID,
	}

	return event
}

func NewProductMoved(
	version SchemaVersion,
	productSKU string,
	fromZoneID string,
	toZoneID string,
	quantity int32,
	sequenceNumber int64,
) WarehouseEvent {
	event := newBaseEvent(version, EventTypeProductMoved, sequenceNumber)
	event.ProductMoved = &ProductMoved{
		ProductSKU: productSKU,
		FromZoneID: fromZoneID,
		ToZoneID:   toZoneID,
		Quantity:   quantity,
	}

	return event
}

func NewProductReserved(
	version SchemaVersion,
	productSKU string,
	orderID string,
	zoneID string,
	quantity int32,
	sequenceNumber int64,
) WarehouseEvent {
	event := newBaseEvent(version, EventTypeProductReserved, sequenceNumber)
	event.ProductReserved = &ProductReserved{
		ProductSKU: productSKU,
		OrderID:    orderID,
		ZoneID:     zoneID,
		Quantity:   quantity,
	}

	return event
}

func NewProductReleased(
	version SchemaVersion,
	productSKU string,
	orderID string,
	zoneID string,
	quantity int32,
	sequenceNumber int64,
) WarehouseEvent {
	event := newBaseEvent(version, EventTypeProductReleased, sequenceNumber)
	event.ProductReleased = &ProductReleased{
		ProductSKU: productSKU,
		OrderID:    orderID,
		ZoneID:     zoneID,
		Quantity:   quantity,
	}

	return event
}

func NewInventoryCounted(
	version SchemaVersion,
	productSKU string,
	zoneID string,
	actualQuantity int32,
	sequenceNumber int64,
) WarehouseEvent {
	event := newBaseEvent(version, EventTypeInventoryCounted, sequenceNumber)
	event.InventoryCounted = &InventoryCounted{
		ProductSKU:     productSKU,
		ZoneID:         zoneID,
		ActualQuantity: actualQuantity,
	}

	return event
}

func NewOrderCreated(
	version SchemaVersion,
	orderID string,
	items []OrderItem,
	sequenceNumber int64,
) WarehouseEvent {
	event := newBaseEvent(version, EventTypeOrderCreated, sequenceNumber)
	event.OrderCreated = &OrderCreated{
		OrderID: orderID,
		Items:   items,
	}

	return event
}

func NewOrderCompleted(
	version SchemaVersion,
	orderID string,
	sequenceNumber int64,
) WarehouseEvent {
	event := newBaseEvent(version, EventTypeOrderCompleted, sequenceNumber)
	event.OrderCompleted = &OrderCompleted{
		OrderID: orderID,
	}

	return event
}

func NewOrderID() string {
	return uuid.NewString()
}

func PartitionKey(event WarehouseEvent) string {
	switch event.EventType {
	case EventTypeProductReceived:
		return productZoneKey(event.ProductReceived.ProductSKU, event.ProductReceived.ZoneID)
	case EventTypeProductShipped:
		return productZoneKey(event.ProductShipped.ProductSKU, event.ProductShipped.ZoneID)
	case EventTypeProductMoved:
		return productZoneKey(event.ProductMoved.ProductSKU, event.ProductMoved.FromZoneID)
	case EventTypeProductReserved:
		return productZoneKey(event.ProductReserved.ProductSKU, event.ProductReserved.ZoneID)
	case EventTypeProductReleased:
		return productZoneKey(event.ProductReleased.ProductSKU, event.ProductReleased.ZoneID)
	case EventTypeInventoryCounted:
		return productZoneKey(event.InventoryCounted.ProductSKU, event.InventoryCounted.ZoneID)
	case EventTypeOrderCreated:
		return event.OrderCreated.OrderID
	case EventTypeOrderCompleted:
		return event.OrderCompleted.OrderID
	default:
		return event.EventID
	}
}

func newBaseEvent(version SchemaVersion, eventType string, sequenceNumber int64) WarehouseEvent {
	return WarehouseEvent{
		EventID:        uuid.NewString(),
		EventType:      eventType,
		SchemaVersion:  int(version),
		SequenceNumber: sequenceNumber,
		OccurredAt:     time.Now().UTC().UnixMilli(),
	}
}

func productZoneKey(productSKU, zoneID string) string {
	return fmt.Sprintf("%s:%s", productSKU, zoneID)
}
