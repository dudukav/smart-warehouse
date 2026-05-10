package main

import (
	"context"
	"os"
	"time"

	"smart_warehouse/internal/config"
	"smart_warehouse/internal/events"
	"smart_warehouse/internal/logger"
	"smart_warehouse/internal/producer"
)

func main() {
	log := logger.New("warehouse-producer")
	cfg := config.LoadProducer()

	schemaV1, err := os.ReadFile(cfg.SchemaV1Path)
	if err != nil {
		log.Error("failed to read v1 schema", "error", err, "path", cfg.SchemaV1Path)
		os.Exit(1)
	}
	schemaV2, err := os.ReadFile(cfg.SchemaV2Path)
	if err != nil {
		log.Error("failed to read v2 schema", "error", err, "path", cfg.SchemaV2Path)
		os.Exit(1)
	}

	encoder, err := events.NewVersionedEncoder(cfg.SchemaRegistryURL, map[int]events.SchemaRegistration{
		int(events.SchemaVersionV1): {
			Subject: cfg.SchemaSubject,
			Schema:  string(schemaV1),
		},
		int(events.SchemaVersionV2): {
			Subject: cfg.SchemaSubject,
			Schema:  string(schemaV2),
		},
	})
	if err != nil {
		log.Error("failed to create avro encoder", "error", err)
		os.Exit(1)
	}

	publisher, err := producer.New(producer.Config{
		BootstrapServers: cfg.KafkaBootstrapServers,
		Topic:            cfg.KafkaTopic,
	}, encoder, log)
	if err != nil {
		log.Error("failed to create producer", "error", err)
		os.Exit(1)
	}
	defer publisher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, event := range demoEvents() {
		if err := publisher.Publish(ctx, event); err != nil {
			log.Error("failed to publish event", "error", err, "event_id", event.EventID, "event_type", event.EventType)
			os.Exit(1)
		}
	}

	invalid := events.NewProductReceived(events.SchemaVersionV2, "SKU-DLQ", "A-01", -5, 100, nil)
	if err := publisher.PublishUnsafe(ctx, invalid); err != nil {
		log.Error("failed to publish invalid dlq demo event", "error", err)
		os.Exit(1)
	}
}

func demoEvents() []events.WarehouseEvent {
	orderID := events.NewOrderID()
	shipmentOrderID := orderID
	supplierID := "SUP-001"

	return []events.WarehouseEvent{
		events.NewProductReceived(events.SchemaVersionV1, "SKU-001", "A-01", 100, 1, nil),
		events.NewProductReceived(events.SchemaVersionV2, "SKU-001", "A-01", 50, 2, &supplierID),
		events.NewProductReserved(events.SchemaVersionV2, "SKU-001", orderID, "A-01", 10, 3),
		events.NewProductReleased(events.SchemaVersionV2, "SKU-001", orderID, "A-01", 5, 4),
		events.NewProductMoved(events.SchemaVersionV2, "SKU-001", "A-01", "B-01", 20, 5),
		events.NewProductShipped(events.SchemaVersionV2, "SKU-001", "B-01", 5, &shipmentOrderID, 6),
		events.NewInventoryCounted(events.SchemaVersionV2, "SKU-001", "B-01", 15, 7),
		events.NewOrderCreated(events.SchemaVersionV2, orderID, []events.OrderItem{
			{
				ProductSKU: "SKU-001",
				ZoneID:     "A-01",
				Quantity:   3,
			},
		}, 8),
		events.NewOrderCompleted(events.SchemaVersionV2, orderID, 9),
		events.NewProductReceived(events.SchemaVersionV2, "SKU-001", "A-01", 999, 1, nil),
	}
}
