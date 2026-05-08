CREATE TABLE IF NOT EXISTS smart_warehouse.processed_events (
  event_id text,
  event_type text,
  processed_at timestamp,
  kafka_partition int,
  kafka_offset bigint,
  PRIMARY KEY ((event_id))
);

CREATE TABLE IF NOT EXISTS smart_warehouse.event_history (
  event_id text,
  occurred_at timestamp,
  event_type text,
  schema_version int,
  sequence_number bigint,
  product_sku text,
  zone_id text,
  order_id text,
  kafka_partition int,
  kafka_offset bigint,
  payload text,
  processed_at timestamp,
  PRIMARY KEY ((event_id))
);
