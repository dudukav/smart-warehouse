CREATE TABLE IF NOT EXISTS smart_warehouse.inventory_by_product_zone (
  product_sku text,
  zone_id text,
  available_quantity int,
  reserved_quantity int,
  last_sequence_number bigint,
  last_event_at timestamp,
  supplier_id text,
  updated_at timestamp,
  PRIMARY KEY ((product_sku, zone_id))
);

CREATE TABLE IF NOT EXISTS smart_warehouse.inventory_by_product (
  product_sku text,
  zone_id text,
  available_quantity int,
  reserved_quantity int,
  last_sequence_number bigint,
  last_event_at timestamp,
  supplier_id text,
  updated_at timestamp,
  PRIMARY KEY ((product_sku), zone_id)
);

CREATE TABLE IF NOT EXISTS smart_warehouse.inventory_by_zone (
  zone_id text,
  product_sku text,
  available_quantity int,
  reserved_quantity int,
  last_sequence_number bigint,
  last_event_at timestamp,
  supplier_id text,
  updated_at timestamp,
  PRIMARY KEY ((zone_id), product_sku)
);
