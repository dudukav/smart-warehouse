# Smart Warehouse

Event-driven система управления складом на Go с Kafka, Avro Schema Registry и Cassandra.

Проект моделирует WMS pipeline: producer публикует события складских операций в Kafka, consumer читает поток событий, валидирует их, поддерживает актуальное состояние склада в Cassandra и отправляет проблемные события в DLQ.

## Возможности

- Kafka topic `warehouse-events` для доменных событий склада.
- Consumer group `warehouse-state-consumer`.
- Manual offset commit после успешной обработки события.
- Avro-схемы v1/v2 и регистрация через Schema Registry.
- Backward-compatible schema evolution для `ProductReceived.supplier_id`.
- Idempotency через таблицу `processed_events`.
- Обработка out-of-order событий через `sequence_number`.
- Cassandra 3-node cluster.
- Keyspace с `NetworkTopologyStrategy` и replication factor `3`.
- Запись с consistency level `QUORUM`.
- Денормализованные Cassandra-таблицы под разные сценарии чтения.
- Cassandra logged batch для атомарного обновления связанных таблиц.
- Dead Letter Queue topic `warehouse-events-dlq`.
- Health endpoint `/health`.
- Prometheus endpoint `/metrics`.
- Prometheus + Grafana dashboard.

## Архитектура

```text
Producer
  -> Avro + Schema Registry
  -> Kafka topic warehouse-events
  -> Consumer
  -> Cassandra state tables

Consumer
  -> invalid/business error events
  -> Kafka topic warehouse-events-dlq

Consumer /metrics
  -> Prometheus
  -> Grafana dashboard
```

## Стек

- Go
- Apache Kafka
- Confluent Schema Registry
- Avro
- Cassandra
- Prometheus
- Grafana
- Docker Compose

## Структура проекта

```text
smart_warehouse/
  cmd/
    consumer/          # entrypoint consumer-сервиса
    producer/          # demo producer
  internal/
    config/            # env config
    consumer/          # Kafka consumer loop, DLQ, commit adapter
    events/            # event types, validation, Avro encoder/decoder, factories
    logger/            # slog logger
    metrics/           # Prometheus metrics wrapper
    producer/          # Kafka producer wrapper
    storage/           # Cassandra storage and event application logic
  migrations/          # Cassandra CQL migrations
  schema/              # Avro schemas v1/v2

monitoring/
  prometheus.yml
  grafana/
    provisioning/
    dashboards/

docker-compose.yml
instructions.md
```

## Запуск

```bash
docker compose up --build
```

Если Cassandra падает с кодом `137`, Docker не хватает памяти. Увеличьте memory limit Docker Desktop и запустите заново:

```bash
docker compose down -v
docker compose up --build
```

## URL

- Consumer health: http://localhost:8080/health
- Consumer metrics: http://localhost:8080/metrics
- Schema Registry: http://localhost:8081
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

Grafana:

```text
admin / admin
```

Dashboard:

```text
Smart Warehouse Consumer
```

## Manual producer

Producer является one-shot CLI: он отправляет вручную переданное событие или массив событий и завершает работу.

Событие можно передать из файла:

```bash
docker compose run --rm producer --file /app/examples/product_received.json
```

Или через stdin:

```bash
docker compose run --rm -T producer < smart_warehouse/examples/product_received.json
```

`event_id`, `occurred_at` и `schema_version` можно не указывать: producer заполнит их сам (`schema_version` по умолчанию `2`).

Для ручной отправки невалидного события в Kafka, например чтобы проверить DLQ, используйте:

```bash
docker compose run --rm -T producer --unsafe < smart_warehouse/examples/product_received_invalid.json
```

Минимальный пример JSON:

```json
{
  "event_type": "PRODUCT_RECEIVED",
  "sequence_number": 1,
  "product_received": {
    "product_sku": "SKU-001",
    "zone_id": "A-01",
    "quantity": 100,
    "supplier_id": "SUP-001"
  }
}
```

## Cassandra model

Таблицы спроектированы под запросы, без JOIN.

### `inventory_by_product_zone`

Точечный запрос остатка товара в зоне:

```sql
SELECT * FROM inventory_by_product_zone
WHERE product_sku = ? AND zone_id = ?;
```

Primary key:

```sql
PRIMARY KEY ((product_sku, zone_id))
```

### `inventory_by_product`

Все зоны, где лежит товар:

```sql
SELECT * FROM inventory_by_product
WHERE product_sku = ?;
```

Primary key:

```sql
PRIMARY KEY ((product_sku), zone_id)
```

### `inventory_by_zone`

Все товары в зоне:

```sql
SELECT * FROM inventory_by_zone
WHERE zone_id = ?;
```

Primary key:

```sql
PRIMARY KEY ((zone_id), product_sku)
```

### Service tables

- `processed_events` — идемпотентность по `event_id`.
- `event_history` — аудит обработанных событий.
- `orders_by_id` — состояние заказа.
- `order_items_by_order` — позиции заказа.

## Event processing

Consumer pipeline:

```text
Kafka message
  -> Avro decode
  -> Validate
  -> processed_events duplicate check
  -> sequence_number stale check
  -> Cassandra logged batch
  -> Kafka offset commit
```

Offset commit выполняется только после успешной записи состояния в Cassandra или после успешной отправки невалидного события в DLQ.

## Domain events

- `PRODUCT_RECEIVED`
- `PRODUCT_SHIPPED`
- `PRODUCT_MOVED`
- `PRODUCT_RESERVED`
- `PRODUCT_RELEASED`
- `INVENTORY_COUNTED`
- `ORDER_CREATED`
- `ORDER_COMPLETED`

## Idempotency

Каждое событие содержит UUID `event_id`. Consumer проверяет таблицу `processed_events`.

Если `event_id` уже существует:

```text
state не меняется
offset коммитится
consumer продолжает работу
```

## Out-of-order handling

Каждое событие содержит `sequence_number`.

В inventory-таблицах хранится:

```text
last_sequence_number
```

Если событие старее уже применённого:

```text
event.sequence_number <= last_sequence_number
```

оно записывается в служебные таблицы, но не меняет остатки.

## DLQ

Невалидные события и бизнес-ошибки отправляются в Kafka topic:

```text
warehouse-events-dlq
```

DLQ-message содержит:

- исходное событие в `original_event_base64`;
- `error_code`;
- `error_reason`;
- `field`;
- `failed_at`;
- Kafka metadata.

Посмотреть DLQ:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic warehouse-events-dlq \
  --from-beginning \
  --max-messages 1
```

## Schema evolution

Схемы:

```text
smart_warehouse/schema/warehouse_event_v1.avsc
smart_warehouse/schema/warehouse_event_v2.avsc
```

V2 добавляет:

```text
product_received.supplier_id
```

Поле nullable и имеет default `null`, поэтому стратегия совместимости:

```text
BACKWARD
```

Compatibility задаётся сервисом `schema-init`.

Проверить Schema Registry:

```bash
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/warehouse-events-value/versions
```

## Monitoring

Consumer отдаёт Prometheus metrics:

- `consumer_lag`
- `events_processed_total`
- `event_processing_duration_seconds`
- `cassandra_write_errors_total`

Prometheus:

```text
http://localhost:9090
```

Grafana:

```text
http://localhost:3000
```

Dashboard:

```text
Smart Warehouse Consumer
```

Панели:

- Consumer lag by partition
- Throughput events/sec
- Cassandra write errors
- Processing duration p95

## Useful checks

Health:

```bash
curl http://localhost:8080/health
```

Metrics:

```bash
curl http://localhost:8080/metrics
```

Cassandra:

```bash
docker compose exec cassandra-1 cqlsh cassandra-1 9042
```

```sql
USE smart_warehouse;
SELECT * FROM inventory_by_product_zone;
SELECT * FROM processed_events;
SELECT * FROM event_history;
```

Cassandra cluster:

```bash
docker compose exec cassandra-1 nodetool status
```

## E2E scenarios

Подробные сценарии проверки находятся в:

```text
instructions.md
```
