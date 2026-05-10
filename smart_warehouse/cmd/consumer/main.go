package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"smart_warehouse/internal/config"
	"smart_warehouse/internal/consumer"
	"smart_warehouse/internal/events"
	"smart_warehouse/internal/logger"
	"smart_warehouse/internal/metrics"
	"smart_warehouse/internal/storage"
)

func main() {
	log := logger.New("warehouse-consumer")
	cfg := config.LoadConsumer()

	storeCfg := storage.DefaultCassandraConfig()
	storeCfg.Hosts = cfg.CassandraHosts
	storeCfg.Keyspace = cfg.CassandraKeyspace
	storeCfg.Consistency = gocql.Quorum

	store, err := storage.NewCassandraStore(&storeCfg)
	if err != nil {
		log.Error("failed to connect cassandra", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	kafkaClient, err := consumer.NewConfluentKafkaClient(consumer.KafkaConfig{
		BootstrapServers: cfg.KafkaBootstrapServers,
		GroupID:          cfg.KafkaGroupID,
		Topic:            cfg.KafkaTopic,
	})
	if err != nil {
		log.Error("failed to create kafka consumer", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := kafkaClient.Close(); err != nil {
			log.Error("failed to close kafka consumer", "error", err)
		}
	}()

	dlq, err := consumer.NewKafkaDLQPublisher(consumer.DLQConfig{
		BootstrapServers: cfg.KafkaBootstrapServers,
		Topic:            cfg.DLQTopic,
	})
	if err != nil {
		log.Error("failed to create dlq publisher", "error", err)
		os.Exit(1)
	}
	defer dlq.Close()

	registry := prometheus.NewRegistry()
	appMetrics := metrics.New(registry)
	httpServer := startHTTPServer(cfg.HTTPAddr, registry, store, log)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Error("failed to shutdown http server", "error", err)
		}
	}()

	handler := consumer.NewHandler(store, log)
	app := consumer.New(
		kafkaClient,
		kafkaClient,
		events.NewDecoder(cfg.SchemaRegistryURL),
		handler,
		dlq,
		appMetrics,
		log,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("consumer stopped with error", "error", err)
		os.Exit(1)
	}
}

func startHTTPServer(addr string, registry *prometheus.Registry, store *storage.CassandraStore, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", healthHandler(store))

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("http server started", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server stopped with error", "error", err)
		}
	}()

	return server
}

func healthHandler(store *storage.CassandraStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := store.Ping(ctx); err != nil {
			http.Error(w, "cassandra unavailable", http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}
