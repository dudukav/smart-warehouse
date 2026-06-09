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
	EventID        string `avro:"event_id" json:"event_id,omitempty"`
	EventType      string `avro:"event_type" json:"event_type"`
	SchemaVersion  int    `avro:"schema_version" json:"schema_version,omitempty"`
	SequenceNumber int64  `avro:"sequence_number" json:"sequence_number"`
	OccurredAt     int64  `avro:"occurred_at" json:"occurred_at,omitempty"`

	ProductReceived  *ProductReceived  `avro:"product_received" json:"product_received,omitempty"`
	ProductShipped   *ProductShipped   `avro:"product_shipped" json:"product_shipped,omitempty"`
	ProductMoved     *ProductMoved     `avro:"product_moved" json:"product_moved,omitempty"`
	ProductReserved  *ProductReserved  `avro:"product_reserved" json:"product_reserved,omitempty"`
	ProductReleased  *ProductReleased  `avro:"product_released" json:"product_released,omitempty"`
	InventoryCounted *InventoryCounted `avro:"inventory_counted" json:"inventory_counted,omitempty"`
	OrderCreated     *OrderCreated     `avro:"order_created" json:"order_created,omitempty"`
	OrderCompleted   *OrderCompleted   `avro:"order_completed" json:"order_completed,omitempty"`
}
