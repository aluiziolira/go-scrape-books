package scraper

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics bundles Prometheus collectors for the scraper.
type Metrics struct {
	Registry          *prometheus.Registry
	RequestsTotal     *prometheus.CounterVec
	RequestDuration   prometheus.Histogram
	ItemsScrapedTotal prometheus.Counter
	RetriesTotal      prometheus.Counter
	ErrorsTotal       *prometheus.CounterVec
}

// NewMetrics constructs and registers all metrics on a dedicated registry.
func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()

	requests := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scraper_requests_total",
			Help: "Total HTTP requests issued by the scraper.",
		},
		[]string{"phase"},
	)
	requestDuration := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "scraper_request_duration_seconds",
			Help:    "HTTP request latency for scraper requests.",
			Buckets: prometheus.DefBuckets,
		},
	)
	itemsScraped := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "scraper_items_scraped_total",
			Help: "Total number of items sent to the pipeline.",
		},
	)
	retries := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "scraper_retries_total",
			Help: "Total number of retry attempts scheduled.",
		},
	)
	errorsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scraper_errors_total",
			Help: "Total number of scraper errors by type.",
		},
		[]string{"error_type"},
	)

	registry.MustRegister(requests, requestDuration, itemsScraped, retries, errorsTotal)

	return &Metrics{
		Registry:          registry,
		RequestsTotal:     requests,
		RequestDuration:   requestDuration,
		ItemsScrapedTotal: itemsScraped,
		RetriesTotal:      retries,
		ErrorsTotal:       errorsTotal,
	}
}

// IncRequest increments the requests total counter.
func (m *Metrics) IncRequest(phase string) {
	if m == nil {
		return
	}
	m.RequestsTotal.WithLabelValues(phase).Inc()
}

// ObserveDuration records an HTTP request duration.
func (m *Metrics) ObserveDuration(d time.Duration) {
	if m == nil {
		return
	}
	m.RequestDuration.Observe(d.Seconds())
}

// IncItems increments the items scraped counter.
func (m *Metrics) IncItems() {
	if m == nil {
		return
	}
	m.ItemsScrapedTotal.Inc()
}

// IncRetries increments the retries counter.
func (m *Metrics) IncRetries() {
	if m == nil {
		return
	}
	m.RetriesTotal.Inc()
}

// IncError increments the errors counter for a type label.
func (m *Metrics) IncError(errorType string) {
	if m == nil {
		return
	}
	m.ErrorsTotal.WithLabelValues(errorType).Inc()
}
