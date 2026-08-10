package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AntonYurchenko/go-intro/internal/retry"
	kafkago "github.com/segmentio/kafka-go"
)

type testReader struct {
	commitErrors []error
	commitCalls  int
}

func (r *testReader) FetchMessage(context.Context) (kafkago.Message, error) {
	return kafkago.Message{}, errors.New("not implemented")
}

func (r *testReader) CommitMessages(context.Context, ...kafkago.Message) error {
	r.commitCalls++
	if r.commitCalls <= len(r.commitErrors) {
		return r.commitErrors[r.commitCalls-1]
	}
	return nil
}

func (r *testReader) Close() error { return nil }

func TestMessageHeadersNormalizesNames(t *testing.T) {
	got := messageHeaders([]kafkago.Header{
		{Key: "TraceParent", Value: []byte("trace")},
		{Key: "X-Request-ID", Value: []byte("request")},
		{Key: "traceparent", Value: []byte("latest")},
	})
	if got["traceparent"] != "latest" {
		t.Fatalf("traceparent = %q", got["traceparent"])
	}
	if got["x-request-id"] != "request" {
		t.Fatalf("x-request-id = %q", got["x-request-id"])
	}
}

func TestCommitEmptyIsNoOp(t *testing.T) {
	consumer := &Consumer{}
	if err := consumer.Commit(context.Background()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestCommitRetries(t *testing.T) {
	reader := &testReader{commitErrors: []error{errors.New("coordinator unavailable")}}
	consumer := &Consumer{
		reader: reader,
		retry: retry.Policy{
			MaxAttempts: 2,
			BaseDelay:   time.Microsecond,
			MaxDelay:    time.Microsecond,
		},
	}
	message := Message{msg: kafkago.Message{Topic: "data", Partition: 0, Offset: 1}}

	if err := consumer.Commit(context.Background(), message); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if reader.commitCalls != 2 {
		t.Fatalf("commit calls = %d, want 2", reader.commitCalls)
	}
}
