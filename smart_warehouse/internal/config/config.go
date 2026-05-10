package config

import (
	"os"
	"strings"
)

type ConsumerConfig struct {
	KafkaBootstrapServers string
	KafkaTopic            string
	KafkaGroupID          string
	DLQTopic              string
	SchemaRegistryURL     string
	CassandraHosts        []string
	CassandraKeyspace     string
	HTTPAddr              string
}

type ProducerConfig struct {
	KafkaBootstrapServers string
	KafkaTopic            string
	SchemaRegistryURL     string
	SchemaSubject         string
	SchemaV1Path          string
	SchemaV2Path          string
}

func LoadConsumer() ConsumerConfig {
	return ConsumerConfig{
		KafkaBootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		KafkaTopic:            getEnv("KAFKA_TOPIC", "warehouse-events"),
		KafkaGroupID:          getEnv("KAFKA_GROUP_ID", "warehouse-state-consumer"),
		DLQTopic:              getEnv("KAFKA_DLQ_TOPIC", "warehouse-events-dlq"),
		SchemaRegistryURL:     getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		CassandraHosts:        splitCSV(getEnv("CASSANDRA_HOSTS", "localhost")),
		CassandraKeyspace:     getEnv("CASSANDRA_KEYSPACE", "smart_warehouse"),
		HTTPAddr:              getEnv("HTTP_ADDR", ":8080"),
	}
}

func LoadProducer() ProducerConfig {
	return ProducerConfig{
		KafkaBootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		KafkaTopic:            getEnv("KAFKA_TOPIC", "warehouse-events"),
		SchemaRegistryURL:     getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		SchemaSubject:         getEnv("SCHEMA_SUBJECT", "warehouse-events-value"),
		SchemaV1Path:          getEnv("SCHEMA_V1_PATH", "schema/warehouse_event_v1.avsc"),
		SchemaV2Path:          getEnv("SCHEMA_V2_PATH", "schema/warehouse_event_v2.avsc"),
	}
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}

	return result
}
