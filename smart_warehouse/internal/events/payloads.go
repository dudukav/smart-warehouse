package events

type ProductReceived struct {
	ProductSKU string  `avro:"product_sku"`
	ZoneID     string  `avro:"zone_id"`
	Quantity   int32   `avro:"quantity"`
	SupplierID *string `avro:"supplier_id"`
}

type ProductShipped struct {
	ProductSKU string  `avro:"product_sku"`
	ZoneID     string  `avro:"zone_id"`
	Quantity   int32   `avro:"quantity"`
	OrderID    *string `avro:"order_id"`
}

type ProductMoved struct {
	ProductSKU string `avro:"product_sku"`
	FromZoneID string `avro:"from_zone_id"`
	ToZoneID   string `avro:"to_zone_id"`
	Quantity   int32  `avro:"quantity"`
}

type ProductReserved struct {
	ProductSKU string `avro:"product_sku"`
	OrderID    string `avro:"order_id"`
	ZoneID     string `avro:"zone_id"`
	Quantity   int32  `avro:"quantity"`
}

type ProductReleased struct {
	ProductSKU string `avro:"product_sku"`
	OrderID    string `avro:"order_id"`
	ZoneID     string `avro:"zone_id"`
	Quantity   int32  `avro:"quantity"`
}

type InventoryCounted struct {
	ProductSKU     string `avro:"product_sku"`
	ZoneID         string `avro:"zone_id"`
	ActualQuantity int32  `avro:"actual_quantity"`
}

type OrderCreated struct {
	OrderID string      `avro:"order_id"`
	Items   []OrderItem `avro:"items"`
}

type OrderCompleted struct {
	OrderID string `avro:"order_id"`
}

type OrderItem struct {
	ProductSKU string `avro:"product_sku"`
	ZoneID     string `avro:"zone_id"`
	Quantity   int32  `avro:"quantity"`
}
