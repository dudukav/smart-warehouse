package events

const (
	EventTypeProductReceived  = "PRODUCT_RECEIVED"
	EventTypeProductShipped   = "PRODUCT_SHIPPED"
	EventTypeProductMoved     = "PRODUCT_MOVED"
	EventTypeProductReserved  = "PRODUCT_RESERVED"
	EventTypeProductReleased  = "PRODUCT_RELEASED"
	EventTypeInventoryCounted = "INVENTORY_COUNTED"
	EventTypeOrderCreated     = "ORDER_CREATED"
	EventTypeOrderCompleted   = "ORDER_COMPLETED"
)

type WarehouseEvent struct {
	EventID        string `avro:"event_id"`
	EventType      string `avro:"event_type"`
	SchemaVersion  int    `avro:"schema_version"`
	SequenceNumber int64  `avro:"sequence_number"`
	OccurredAt     int64  `avro:"occurred_at"`

	ProductReceived  *ProductReceived  `avro:"product_received"`
	ProductShipped   *ProductShipped   `avro:"product_shipped"`
	ProductMoved     *ProductMoved     `avro:"product_moved"`
	ProductReserved  *ProductReserved  `avro:"product_reserved"`
	ProductReleased  *ProductReleased  `avro:"product_released"`
	InventoryCounted *InventoryCounted `avro:"inventory_counted"`
	OrderCreated     *OrderCreated     `avro:"order_created"`
	OrderCompleted   *OrderCompleted   `avro:"order_completed"`
}
