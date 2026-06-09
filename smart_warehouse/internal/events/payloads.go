package events

type ProductReceived struct {
	ProductSKU string  `avro:"product_sku" json:"product_sku"`
	ZoneID     string  `avro:"zone_id" json:"zone_id"`
	Quantity   int32   `avro:"quantity" json:"quantity"`
	SupplierID *string `avro:"supplier_id" json:"supplier_id,omitempty"`
}

type ProductShipped struct {
	ProductSKU string  `avro:"product_sku" json:"product_sku"`
	ZoneID     string  `avro:"zone_id" json:"zone_id"`
	Quantity   int32   `avro:"quantity" json:"quantity"`
	OrderID    *string `avro:"order_id" json:"order_id,omitempty"`
}

type ProductMoved struct {
	ProductSKU string `avro:"product_sku" json:"product_sku"`
	FromZoneID string `avro:"from_zone_id" json:"from_zone_id"`
	ToZoneID   string `avro:"to_zone_id" json:"to_zone_id"`
	Quantity   int32  `avro:"quantity" json:"quantity"`
}

type ProductReserved struct {
	ProductSKU string `avro:"product_sku" json:"product_sku"`
	OrderID    string `avro:"order_id" json:"order_id"`
	ZoneID     string `avro:"zone_id" json:"zone_id"`
	Quantity   int32  `avro:"quantity" json:"quantity"`
}

type ProductReleased struct {
	ProductSKU string `avro:"product_sku" json:"product_sku"`
	OrderID    string `avro:"order_id" json:"order_id"`
	ZoneID     string `avro:"zone_id" json:"zone_id"`
	Quantity   int32  `avro:"quantity" json:"quantity"`
}

type InventoryCounted struct {
	ProductSKU     string `avro:"product_sku" json:"product_sku"`
	ZoneID         string `avro:"zone_id" json:"zone_id"`
	ActualQuantity int32  `avro:"actual_quantity" json:"actual_quantity"`
}

type OrderCreated struct {
	OrderID string      `avro:"order_id" json:"order_id"`
	Items   []OrderItem `avro:"items" json:"items"`
}

type OrderCompleted struct {
	OrderID string `avro:"order_id" json:"order_id"`
}

type OrderItem struct {
	ProductSKU string `avro:"product_sku" json:"product_sku"`
	ZoneID     string `avro:"zone_id" json:"zone_id"`
	Quantity   int32  `avro:"quantity" json:"quantity"`
}
