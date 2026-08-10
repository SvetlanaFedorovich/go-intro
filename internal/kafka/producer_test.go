package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AntonYurchenko/go-intro/internal/retry"
	kafkago "github.com/segmentio/kafka-go"
)

type testWriter struct {
	errors   []error
	calls    int
	messages []kafkago.Message
}

func (w *testWriter) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	w.calls++
	w.messages = append(w.messages, messages...)
	if w.calls <= len(w.errors) {
		return w.errors[w.calls-1]
	}
	return nil
}

func (w *testWriter) Close() error { return nil }

func TestProducerRetriesPublishAndPreservesMessage(t *testing.T) {
	writer := &testWriter{errors: []error{errors.New("first"), errors.New("second")}}
	producer := &Producer{
		writer: writer,
		topic:  "data",
		retry: retry.Policy{
			MaxAttempts: 3,
			BaseDelay:   time.Microsecond,
			MaxDelay:    time.Microsecond,
		},
	}

	err := producer.PublishKeyed(
		context.Background(),
		[]byte("event-1"),
		[]byte("payload"),
		map[string]string{"traceparent": "trace"},
	)
	if err != nil {
		t.Fatalf("PublishKeyed: %v", err)
	}
	if writer.calls != 3 {
		t.Fatalf("writer calls = %d, want 3", writer.calls)
	}
	last := writer.messages[len(writer.messages)-1]
	if string(last.Key) != "event-1" || string(last.Value) != "payload" {
		t.Fatalf("message = %+v", last)
	}
	if len(last.Headers) != 1 || last.Headers[0].Key != "traceparent" {
		t.Fatalf("headers = %+v", last.Headers)
	}
}

func TestProducerReturnsErrorAfterMaxAttempts(t *testing.T) {
	wantErr := errors.New("unavailable")
	writer := &testWriter{errors: []error{wantErr, wantErr, wantErr}}
	producer := &Producer{
		writer: writer,
		topic:  "data",
		retry: retry.Policy{
			MaxAttempts: 2,
			BaseDelay:   time.Microsecond,
			MaxDelay:    time.Microsecond,
		},
	}

	err := producer.Publish(context.Background(), []byte("payload"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if writer.calls != 2 {
		t.Fatalf("writer calls = %d, want 2", writer.calls)
	}
}
