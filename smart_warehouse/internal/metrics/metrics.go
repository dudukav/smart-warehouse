package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	eventsProcessed      *prometheus.CounterVec
	processingDuration   *prometheus.HistogramVec
	cassandraWriteErrors prometheus.Counter
	consumerLag          *prometheus.GaugeVec
}

func New(registry *prometheus.Registry) *Metrics {
	m := &Metrics{
		eventsProcessed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "events_processed_total",
				Help: "Total number of successfully processed warehouse events.",
			},
			[]string{"event_type"},
		),

		processingDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "event_processing_duration_seconds",
				Help:    "Warehouse event processing duration in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"event_type"},
		),

		cassandraWriteErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "cassandra_write_errors_total",
				Help: "Total number of Cassandra write errors.",
			},
		),

		consumerLag: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "consumer_lag",
				Help: "Consumer lag by topic and partition.",
			},
			[]string{"topic", "partition"},
		),
	}

	registry.MustRegister(
		m.eventsProcessed,
		m.processingDuration,
		m.cassandraWriteErrors,
		m.consumerLag,
	)

	return m
}

func (m *Metrics) EventProcessed(eventType string) {
	if m == nil || m.eventsProcessed == nil {
		return
	}

	m.eventsProcessed.WithLabelValues(eventType).Inc()
}

func (m *Metrics) ObserveProcessingDuration(eventType string, duration time.Duration) {
	if m == nil || m.processingDuration == nil {
		return
	}

	m.processingDuration.WithLabelValues(eventType).Observe(duration.Seconds())
}

func (m *Metrics) CassandraWriteError() {
	if m == nil || m.cassandraWriteErrors == nil {
		return
	}

	m.cassandraWriteErrors.Inc()
}

func (m *Metrics) SetConsumerLag(topic string, partition int32, lag int64) {
	if m == nil || m.consumerLag == nil {
		return
	}
	if lag < 0 {
		lag = 0
	}

	m.consumerLag.
		WithLabelValues(topic, strconv.FormatInt(int64(partition), 10)).
		Set(float64(lag))
}
