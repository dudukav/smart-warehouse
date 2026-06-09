package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"smart_warehouse/internal/config"
	"smart_warehouse/internal/events"
	"smart_warehouse/internal/logger"
	"smart_warehouse/internal/producer"
)

func main() {
	eventFile := flag.String("file", "-", "path to JSON event file, or - to read from stdin")
	unsafePublish := flag.Bool("unsafe", false, "publish without producer-side validation")
	flag.Parse()

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

	manualEvents, err := readManualEvents(*eventFile)
	if err != nil {
		log.Error("failed to read manual events", "error", err)
		os.Exit(1)
	}

	for _, event := range manualEvents {
		err := publisher.Publish(ctx, event)
		if *unsafePublish {
			err = publisher.PublishUnsafe(ctx, event)
		}
		if err != nil {
			log.Error("failed to publish event", "error", err, "event_id", event.EventID, "event_type", event.EventType)
			os.Exit(1)
		}
	}
}

func readManualEvents(path string) ([]events.WarehouseEvent, error) {
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, err
	}

	data = []byte(strings.TrimSpace(string(data)))
	if len(data) == 0 {
		return nil, fmt.Errorf("event JSON is empty")
	}

	var result []events.WarehouseEvent
	if data[0] == '[' {
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, fmt.Errorf("decode event array: %w", err)
		}
	} else {
		var event events.WarehouseEvent
		if err := json.Unmarshal(data, &event); err != nil {
			return nil, fmt.Errorf("decode event: %w", err)
		}
		result = append(result, event)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("event JSON must contain at least one event")
	}

	now := time.Now().UTC().UnixMilli()
	for index := range result {
		fillManualEventDefaults(&result[index], now)
	}

	return result, nil
}

func fillManualEventDefaults(event *events.WarehouseEvent, occurredAt int64) {
	if event.EventID == "" {
		event.EventID = uuid.NewString()
	}
	if event.SchemaVersion == 0 {
		event.SchemaVersion = int(events.SchemaVersionV2)
	}
	if event.OccurredAt == 0 {
		event.OccurredAt = occurredAt
	}
}
