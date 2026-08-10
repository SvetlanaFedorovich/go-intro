package observability

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestHTTPMiddlewareRecordsStatusAndReleasesInFlight(t *testing.T) {
	counter := httpRequests.WithLabelValues("TEST", "/metrics-test", "204")
	before := testutil.ToFloat64(counter)
	handler := HTTPMiddleware("/metrics-test", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if got := testutil.ToFloat64(httpInFlight.WithLabelValues("TEST", "/metrics-test")); got != 1 {
			t.Fatalf("in-flight inside handler = %v, want 1", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	handler.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("TEST", "/metrics-test", nil),
	)

	if got := testutil.ToFloat64(counter); got != before+1 {
		t.Fatalf("request counter = %v, want %v", got, before+1)
	}
	if got := testutil.ToFloat64(httpInFlight.WithLabelValues("TEST", "/metrics-test")); got != 0 {
		t.Fatalf("in-flight after handler = %v, want 0", got)
	}
}

func TestObserveKafkaPublish(t *testing.T) {
	success := kafkaPublish.WithLabelValues("metrics-test", "success")
	failure := kafkaPublish.WithLabelValues("metrics-test", "error")
	successBefore := testutil.ToFloat64(success)
	failureBefore := testutil.ToFloat64(failure)

	ObserveKafkaPublish("metrics-test", time.Now(), nil)
	ObserveKafkaPublish("metrics-test", time.Now(), errors.New("failed"))

	if got := testutil.ToFloat64(success); got != successBefore+1 {
		t.Fatalf("success counter = %v", got)
	}
	if got := testutil.ToFloat64(failure); got != failureBefore+1 {
		t.Fatalf("failure counter = %v", got)
	}
}

func TestObserveMessageAndConsumerLag(t *testing.T) {
	counter := workerMessages.WithLabelValues("metrics-test")
	before := testutil.ToFloat64(counter)
	ObserveMessage("metrics-test", time.Now())
	if got := testutil.ToFloat64(counter); got != before+1 {
		t.Fatalf("message counter = %v", got)
	}

	SetConsumerLag("metrics-test", "7", 42)
	if got := testutil.ToFloat64(consumerLag.WithLabelValues("metrics-test", "7")); got != 42 {
		t.Fatalf("consumer lag = %v, want 42", got)
	}
}

func TestObserveEventLatencySkipsZeroTime(t *testing.T) {
	before := histogramCount(t, workerEventLatency)
	ObserveEventLatency(time.Time{})
	if got := histogramCount(t, workerEventLatency); got != before {
		t.Fatalf("count after zero time = %d, want %d", got, before)
	}
	ObserveEventLatency(time.Now().Add(-time.Second))
	if got := histogramCount(t, workerEventLatency); got != before+1 {
		t.Fatalf("count after observation = %d, want %d", got, before+1)
	}
}

func TestObserveDLQ(t *testing.T) {
	success := dlqMessages.WithLabelValues("success")
	failure := dlqMessages.WithLabelValues("error")
	successBefore := testutil.ToFloat64(success)
	failureBefore := testutil.ToFloat64(failure)

	ObserveDLQ(nil)
	ObserveDLQ(errors.New("failed"))

	if got := testutil.ToFloat64(success); got != successBefore+1 {
		t.Fatalf("success DLQ counter = %v", got)
	}
	if got := testutil.ToFloat64(failure); got != failureBefore+1 {
		t.Fatalf("failure DLQ counter = %v", got)
	}
}

type histogramWriter interface {
	Write(*dto.Metric) error
}

func histogramCount(t *testing.T, histogram histogramWriter) uint64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := histogram.Write(metric); err != nil {
		t.Fatalf("write histogram: %v", err)
	}
	return metric.GetHistogram().GetSampleCount()
}
