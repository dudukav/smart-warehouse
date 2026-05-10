package events

import (
	"encoding/binary"
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
)

const (
	confluentMagicByte   byte = 0
	confluentAvroPayload      = 5
	confluentSchemaID         = 1
)

type Decoder struct {
	registry *srclient.SchemaRegistryClient
}

type Encoder struct {
	registry *srclient.SchemaRegistryClient
	schemas  map[int]registeredSchema
}

type registeredSchema struct {
	id     int
	schema avro.Schema
}

func NewDecoder(schemaRegistryURL string) *Decoder {
	return &Decoder{
		registry: srclient.CreateSchemaRegistryClient(schemaRegistryURL),
	}
}

func NewEncoder(schemaRegistryURL, subject, schemaText string) (*Encoder, error) {
	return NewVersionedEncoder(schemaRegistryURL, map[int]SchemaRegistration{
		int(SchemaVersionV2): {
			Subject: subject,
			Schema:  schemaText,
		},
	})
}

type SchemaRegistration struct {
	Subject string
	Schema  string
}

func NewVersionedEncoder(schemaRegistryURL string, registrations map[int]SchemaRegistration) (*Encoder, error) {
	registry := srclient.CreateSchemaRegistryClient(schemaRegistryURL)
	encoder := &Encoder{
		registry: registry,
		schemas:  make(map[int]registeredSchema, len(registrations)),
	}

	for version, registration := range registrations {
		registered, err := registry.CreateSchema(registration.Subject, registration.Schema, srclient.Avro)
		if err != nil {
			return nil, fmt.Errorf("register avro schema subject=%s version=%d: %w", registration.Subject, version, err)
		}

		parsed, err := avro.Parse(registration.Schema)
		if err != nil {
			return nil, fmt.Errorf("parse avro schema subject=%s version=%d: %w", registration.Subject, version, err)
		}

		encoder.schemas[version] = registeredSchema{
			id:     registered.ID(),
			schema: parsed,
		}
	}

	return encoder, nil
}

func (d *Decoder) Decode(data []byte) (*WarehouseEvent, error) {
	if len(data) < 5 {
		return nil, NewValidationError(
			"INVALID_AVRO_MESSAGE",
			"message",
			"message is too short for Confluent Avro wire format",
		)
	}

	if data[0] != confluentMagicByte {
		return nil, NewValidationError(
			"INVALID_AVRO_MESSAGE",
			"message",
			"invalid Confluent Avro magic byte",
		)
	}

	schemaID := int(binary.BigEndian.Uint32(data[1:5]))

	schema, err := d.registry.GetSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("get schema %d from registry: %w", schemaID, err)
	}

	avroSchema, err := avro.Parse(schema.Schema())
	if err != nil {
		return nil, fmt.Errorf("parse avro schema %d: %w", schemaID, err)
	}

	var event WarehouseEvent
	if err := avro.Unmarshal(avroSchema, data[5:], &event); err != nil {
		return nil, NewValidationError(
			"AVRO_DECODE_ERROR",
			"message",
			fmt.Sprintf("failed to decode Avro payload with schema %d: %v", schemaID, err),
		)
	}

	return &event, nil
}

func (e *Encoder) Encode(event *WarehouseEvent) ([]byte, error) {
	if err := event.Validate(); err != nil {
		return nil, fmt.Errorf("encode avro event: %w", err)
	}

	schema, ok := e.schemas[event.SchemaVersion]
	if !ok {
		return nil, NewValidationError("UNSUPPORTED_SCHEMA_VERSION", "schema_version", fmt.Sprintf("schema version %d is not registered in encoder", event.SchemaVersion))
	}

	payload, err := avro.Marshal(schema.schema, event)
	if err != nil {
		return nil, fmt.Errorf("marshal avro event: %w", err)
	}

	data := make([]byte, confluentAvroPayload+len(payload))
	data[0] = confluentMagicByte
	binary.BigEndian.PutUint32(data[confluentSchemaID:confluentAvroPayload], uint32(schema.id))
	copy(data[confluentAvroPayload:], payload)
	return data, nil
}
