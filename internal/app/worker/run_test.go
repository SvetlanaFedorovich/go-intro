package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AntonYurchenko/go-intro/internal/kafka"
	"github.com/AntonYurchenko/go-intro/internal/model"
	"github.com/AntonYurchenko/go-intro/internal/store"
	kafkago "github.com/segmentio/kafka-go"
)

func TestPositiveDuration(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{name: "hours", value: "168h", want: 168 * time.Hour},
		{name: "minutes", value: "30m", want: 30 * time.Minute},
		{name: "zero", value: "0s", wantErr: true},
		{name: "negative", value: "-1h", wantErr: true},
		{name: "invalid", value: "weekly", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := positiveDuration("TEST_DURATION", tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("duration = %s, want %s", got, tt.want)
			}
		})
	}
}

type storeResult struct {
	inserted bool
	err      error
}

type testStore struct {
	mu      sync.Mutex
	results map[string]storeResult
	calls   []string
	data    []model.Data
}

func (s *testStore) InsertOnce(_ context.Context, eventID, _ string, _ int, _ int64, data model.Data) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, eventID)
	s.data = append(s.data, data)
	if result, ok := s.results[eventID]; ok {
		return result.inserted, result.err
	}
	return true, nil
}

func (s *testStore) CleanupProcessed(context.Context, time.Time) (int64, error) { return 0, nil }
func (s *testStore) Ping(context.Context) error                                 { return nil }
func (s *testStore) Close()                                                     {}

type publishedMessage struct {
	value   []byte
	headers []kafkago.Header
}

type testDLQ struct {
	mu       sync.Mutex
	err      error
	messages []publishedMessage
}

func (d *testDLQ) PublishWithHeaders(_ context.Context, value []byte, headers []kafkago.Header) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.messages = append(d.messages, publishedMessage{
		value:   append([]byte(nil), value...),
		headers: append([]kafkago.Header(nil), headers...),
	})
	return d.err
}

type testConsumer struct {
	messages  []kafka.Message
	fetchErr  error
	commitErr error
	committed []kafka.Message
}

func (c *testConsumer) FetchBatch(context.Context, int) ([]kafka.Message, error) {
	return append([]kafka.Message(nil), c.messages...), c.fetchErr
}

func (c *testConsumer) Commit(_ context.Context, messages ...kafka.Message) error {
	c.committed = append([]kafka.Message(nil), messages...)
	return c.commitErr
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func eventMessage(eventID string, partition int, offset int64) kafka.Message {
	return kafka.Message{
		Topic:     "data",
		Partition: partition,
		Offset:    offset,
		Time:      time.Now(),
		Value: []byte(`{"event_id":"` + eventID +
			`","user":"Max","age":31,"email":"max@example.com"}`),
	}
}

func TestHandleMessageSaved(t *testing.T) {
	db := &testStore{}
	dlq := &testDLQ{}

	err := handleMessage(context.Background(), dlq, db, testLogger(), eventMessage("event-1", 2, 42))
	if err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if len(db.calls) != 1 || db.calls[0] != "event-1" {
		t.Fatalf("store calls = %v", db.calls)
	}
	if len(db.data) != 1 || db.data[0].Email != "max@example.com" {
		t.Fatalf("stored data = %+v", db.data)
	}
	if len(dlq.messages) != 0 {
		t.Fatalf("DLQ messages = %d, want 0", len(dlq.messages))
	}
}

func TestHandleMessageDuplicateIsSuccessful(t *testing.T) {
	db := &testStore{results: map[string]storeResult{
		"event-1": {inserted: false},
	}}

	if err := handleMessage(context.Background(), &testDLQ{}, db, testLogger(), eventMessage("event-1", 0, 1)); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
}

func TestHandleMessageLegacyEventID(t *testing.T) {
	db := &testStore{}
	message := eventMessage("", 3, 9)
	message.Value = []byte(`{"user":"Max","age":31,"email":"max@example.com"}`)

	if err := handleMessage(context.Background(), &testDLQ{}, db, testLogger(), message); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if len(db.calls) != 1 || db.calls[0] != "legacy:data:3:9" {
		t.Fatalf("store calls = %v", db.calls)
	}
}

func TestHandleMessageRejectsInvalidJSONToDLQ(t *testing.T) {
	dlq := &testDLQ{}
	message := kafka.Message{
		Topic:     "data",
		Partition: 4,
		Offset:    17,
		Value:     []byte(`{`),
		Headers:   map[string]string{"x-request-id": "request-17"},
	}

	if err := handleMessage(context.Background(), dlq, &testStore{}, testLogger(), message); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if len(dlq.messages) != 1 {
		t.Fatalf("DLQ messages = %d, want 1", len(dlq.messages))
	}
	headers := headersMap(dlq.messages[0].headers)
	if headers["source_topic"] != "data" || headers["source_partition"] != "4" || headers["source_offset"] != "17" {
		t.Fatalf("DLQ headers = %v", headers)
	}
	if headers["error"] == "" {
		t.Fatal("DLQ error header is empty")
	}
	if headers["x-request-id"] != "request-17" {
		t.Fatalf("DLQ request ID = %q", headers["x-request-id"])
	}
}

func TestHandleMessageRejectsInvalidPayloadAndEventID(t *testing.T) {
	tests := []struct {
		name    string
		message kafka.Message
	}{
		{
			name: "invalid payload",
			message: kafka.Message{
				Topic: "data",
				Value: []byte(`{"event_id":"event-1","user":"Max","age":0,"email":"max@example.com"}`),
			},
		},
		{
			name: "invalid event id",
			message: kafka.Message{
				Topic: "data",
				Value: []byte(`{"event_id":"` + strings.Repeat("x", 129) +
					`","user":"Max","age":31,"email":"max@example.com"}`),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dlq := &testDLQ{}
			if err := handleMessage(context.Background(), dlq, &testStore{}, testLogger(), tt.message); err != nil {
				t.Fatalf("handleMessage: %v", err)
			}
			if len(dlq.messages) != 1 {
				t.Fatalf("DLQ messages = %d, want 1", len(dlq.messages))
			}
		})
	}
}

func TestHandleMessageConflictGoesToDLQ(t *testing.T) {
	db := &testStore{results: map[string]storeResult{
		"event-1": {err: store.ErrEventConflict},
	}}
	dlq := &testDLQ{}

	if err := handleMessage(context.Background(), dlq, db, testLogger(), eventMessage("event-1", 0, 1)); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if len(dlq.messages) != 1 {
		t.Fatalf("DLQ messages = %d, want 1", len(dlq.messages))
	}
}

func TestHandleMessageDLQFailureIsReturned(t *testing.T) {
	wantErr := errors.New("dlq unavailable")
	dlq := &testDLQ{err: wantErr}
	message := kafka.Message{Topic: "data", Value: []byte(`not-json`)}

	err := handleMessage(context.Background(), dlq, &testStore{}, testLogger(), message)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestHandleMessageStoreFailureDoesNotGoToDLQ(t *testing.T) {
	wantErr := errors.New("postgres unavailable")
	db := &testStore{results: map[string]storeResult{
		"event-1": {err: wantErr},
	}}
	dlq := &testDLQ{}

	err := handleMessage(context.Background(), dlq, db, testLogger(), eventMessage("event-1", 0, 1))
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if len(dlq.messages) != 0 {
		t.Fatalf("DLQ messages = %d, want 0", len(dlq.messages))
	}
}

func TestProcessBatchCommitsLatestOffsetPerPartition(t *testing.T) {
	consumer := &testConsumer{messages: []kafka.Message{
		eventMessage("a", 0, 10),
		eventMessage("b", 1, 20),
		eventMessage("c", 0, 11),
		eventMessage("d", 1, 21),
	}}

	if err := processBatch(context.Background(), consumer, &testDLQ{}, &testStore{}, testLogger()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	assertOffsets(t, consumer.committed, map[int]int64{0: 11, 1: 21})
}

func TestProcessBatchCommitsOnlySuccessfulPrefix(t *testing.T) {
	wantErr := errors.New("store failed")
	db := &testStore{results: map[string]storeResult{
		"failed": {err: wantErr},
	}}
	consumer := &testConsumer{messages: []kafka.Message{
		eventMessage("a", 0, 10),
		eventMessage("b", 1, 20),
		eventMessage("failed", 0, 11),
		eventMessage("after", 1, 21),
	}}

	err := processBatch(context.Background(), consumer, &testDLQ{}, db, testLogger())
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	assertOffsets(t, consumer.committed, map[int]int64{0: 10, 1: 20})
}

func TestProcessBatchProcessesMessagesBeforeReturningFetchError(t *testing.T) {
	wantErr := errors.New("fetch interrupted")
	consumer := &testConsumer{
		messages: []kafka.Message{eventMessage("a", 0, 1)},
		fetchErr: wantErr,
	}
	db := &testStore{}

	err := processBatch(context.Background(), consumer, &testDLQ{}, db, testLogger())
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if len(db.calls) != 1 {
		t.Fatalf("store calls = %d, want 1", len(db.calls))
	}
	assertOffsets(t, consumer.committed, map[int]int64{0: 1})
}

func TestProcessBatchCommitFailureIsReturned(t *testing.T) {
	wantErr := errors.New("commit failed")
	consumer := &testConsumer{
		messages:  []kafka.Message{eventMessage("a", 0, 1)},
		commitErr: wantErr,
	}

	err := processBatch(context.Background(), consumer, &testDLQ{}, &testStore{}, testLogger())
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestPartitionOffsets(t *testing.T) {
	got := partitionOffsets([]kafka.Message{
		{Topic: "b", Partition: 1, Offset: 4},
		{Topic: "a", Partition: 2, Offset: 8},
		{Topic: "a", Partition: 2, Offset: 9},
		{Topic: "a", Partition: 0, Offset: 3},
	})
	if len(got) != 3 {
		t.Fatalf("offset count = %d, want 3", len(got))
	}
	if got[0].Topic != "a" || got[0].Partition != 0 || got[0].Offset != 3 ||
		got[1].Topic != "a" || got[1].Partition != 2 || got[1].Offset != 9 ||
		got[2].Topic != "b" || got[2].Partition != 1 || got[2].Offset != 4 {
		t.Fatalf("offsets = %+v", got)
	}
}

func headersMap(headers []kafkago.Header) map[string]string {
	result := make(map[string]string, len(headers))
	for _, header := range headers {
		result[header.Key] = string(header.Value)
	}
	return result
}

func assertOffsets(t *testing.T, messages []kafka.Message, want map[int]int64) {
	t.Helper()
	if len(messages) != len(want) {
		t.Fatalf("committed = %+v, want %v", messages, want)
	}
	for _, message := range messages {
		if want[message.Partition] != message.Offset {
			t.Fatalf("committed partition %d offset %d, want %d", message.Partition, message.Offset, want[message.Partition])
		}
	}
}
