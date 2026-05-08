CREATE TABLE IF NOT EXISTS smart_warehouse.orders_by_id (
  order_id text,
  status text,
  created_at timestamp,
  completed_at timestamp,
  last_sequence_number bigint,
  updated_at timestamp,
  PRIMARY KEY ((order_id))
);

CREATE TABLE IF NOT EXISTS smart_warehouse.order_items_by_order (
  order_id text,
  product_sku text,
  zone_id text,
  quantity int,
  PRIMARY KEY ((order_id), product_sku, zone_id)
);
