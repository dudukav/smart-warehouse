package events

import (
	"encoding/binary"
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
)

const confluentMagicByte byte = 0

type Decoder struct {
	registry *srclient.SchemaRegistryClient
}

func NewDecoder(schemaRegistryURL string) *Decoder {
	return &Decoder{
		registry: srclient.CreateSchemaRegistryClient(schemaRegistryURL),
	}
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
