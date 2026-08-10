package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "go_intro",
		Subsystem: "api",
		Name:      "http_requests_total",
		Help:      "Total number of HTTP requests.",
	}, []string{"method", "route", "status"})

	httpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "go_intro",
		Subsystem: "api",
		Name:      "http_request_duration_seconds",
		Help:      "HTTP request duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "route"})

	httpInFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "go_intro",
		Subsystem: "api",
		Name:      "http_in_flight_requests",
		Help:      "Number of HTTP requests currently being processed.",
	}, []string{"method", "route"})

	kafkaPublish = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "go_intro",
		Subsystem: "kafka",
		Name:      "publish_total",
		Help:      "Kafka publish attempts by topic and result.",
	}, []string{"topic", "result"})

	kafkaPublishDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "go_intro",
		Subsystem: "kafka",
		Name:      "publish_duration_seconds",
		Help:      "Kafka synchronous publish duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"topic"})

	workerMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "go_intro",
		Subsystem: "worker",
		Name:      "messages_total",
		Help:      "Worker messages by processing outcome.",
	}, []string{"outcome"})

	workerMessageDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "go_intro",
		Subsystem: "worker",
		Name:      "message_duration_seconds",
		Help:      "Worker processing duration per message.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"outcome"})

	workerEventLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "go_intro",
		Subsystem: "worker",
		Name:      "event_end_to_end_latency_seconds",
		Help:      "Time from Kafka publication to completion of worker processing.",
		Buckets:   []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	})

	workerBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "go_intro",
		Subsystem: "worker",
		Name:      "batch_size",
		Help:      "Number of messages fetched in a worker batch.",
		Buckets:   []float64{1, 5, 10, 25, 50, 100},
	})

	workerBatchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "go_intro",
		Subsystem: "worker",
		Name:      "batch_duration_seconds",
		Help:      "Worker batch processing duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	})

	consumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "go_intro",
		Subsystem: "worker",
		Name:      "consumer_lag",
		Help:      "Kafka consumer lag reported by kafka-go.",
	}, []string{"topic", "partition"})

	dlqMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "go_intro",
		Subsystem: "worker",
		Name:      "dlq_total",
		Help:      "Messages sent to DLQ by result.",
	}, []string{"result"})

	retries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "go_intro",
		Name:      "retries_total",
		Help:      "Retry attempts by operation.",
	}, []string{"operation"})

	processedCleanup = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "go_intro",
		Subsystem: "worker",
		Name:      "processed_events_cleanup_total",
		Help:      "Total processed event ledger rows removed by retention cleanup.",
	})
)

func init() {
	prometheus.MustRegister(
		httpRequests,
		httpDuration,
		httpInFlight,
		kafkaPublish,
		kafkaPublishDuration,
		workerMessages,
		workerMessageDuration,
		workerEventLatency,
		workerBatchSize,
		workerBatchDuration,
		consumerLag,
		dlqMessages,
		retries,
		processedCleanup,
	)
}

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

func HTTPMiddleware(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		inFlight := httpInFlight.WithLabelValues(r.Method, route)
		inFlight.Inc()
		defer inFlight.Dec()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		httpRequests.WithLabelValues(r.Method, route, strconv.Itoa(recorder.status)).Inc()
		httpDuration.WithLabelValues(r.Method, route).Observe(time.Since(started).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusRecorder) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *statusRecorder) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func ObserveKafkaPublish(topic string, started time.Time, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	kafkaPublish.WithLabelValues(topic, result).Inc()
	kafkaPublishDuration.WithLabelValues(topic).Observe(time.Since(started).Seconds())
}

func ObserveBatch(size int, started time.Time) {
	workerBatchSize.Observe(float64(size))
	workerBatchDuration.Observe(time.Since(started).Seconds())
}

func ObserveMessage(outcome string, started time.Time) {
	workerMessages.WithLabelValues(outcome).Inc()
	workerMessageDuration.WithLabelValues(outcome).Observe(time.Since(started).Seconds())
}

func ObserveEventLatency(publishedAt time.Time) {
	if publishedAt.IsZero() {
		return
	}
	latency := time.Since(publishedAt).Seconds()
	if latency >= 0 {
		workerEventLatency.Observe(latency)
	}
}

func SetConsumerLag(topic, partition string, lag int64) {
	consumerLag.WithLabelValues(topic, partition).Set(float64(lag))
}

func ObserveDLQ(err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	dlqMessages.WithLabelValues(result).Inc()
}

func ObserveRetry(operation string) {
	retries.WithLabelValues(operation).Inc()
}

func AddProcessedCleanup(deleted int64) {
	processedCleanup.Add(float64(deleted))
}
