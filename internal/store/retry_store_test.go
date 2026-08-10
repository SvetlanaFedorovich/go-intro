package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AntonYurchenko/go-intro/internal/model"
	"github.com/AntonYurchenko/go-intro/internal/retry"
	"github.com/jackc/pgx/v5/pgconn"
)

type retryTestStore struct {
	insertErrors []error
	insertCalls  int
}

func (s *retryTestStore) InsertOnce(context.Context, string, string, int, int64, model.Data) (bool, error) {
	s.insertCalls++
	if s.insertCalls <= len(s.insertErrors) {
		return false, s.insertErrors[s.insertCalls-1]
	}
	return true, nil
}

func (s *retryTestStore) CleanupProcessed(context.Context, time.Time) (int64, error) {
	return 0, nil
}
func (s *retryTestStore) Ping(context.Context) error { return nil }
func (s *retryTestStore) Close()                     {}

func TestRetryStoreRetriesTransientInsert(t *testing.T) {
	temporary := &pgconn.PgError{Code: "40001", Message: "serialization failure"}
	next := &retryTestStore{insertErrors: []error{temporary, temporary}}
	db := &retryStore{
		next: next,
		policy: retry.Policy{
			MaxAttempts: 3,
			BaseDelay:   time.Microsecond,
			MaxDelay:    time.Microsecond,
		},
	}

	inserted, err := db.InsertOnce(context.Background(), "event", "data", 0, 1, model.Data{})
	if err != nil {
		t.Fatalf("InsertOnce: %v", err)
	}
	if !inserted || next.insertCalls != 3 {
		t.Fatalf("inserted=%v calls=%d, want true/3", inserted, next.insertCalls)
	}
}

func TestRetryStoreDoesNotRetryConflict(t *testing.T) {
	next := &retryTestStore{insertErrors: []error{ErrEventConflict}}
	db := &retryStore{
		next:   next,
		policy: retry.DefaultPolicy(),
	}

	_, err := db.InsertOnce(context.Background(), "event", "data", 0, 1, model.Data{})
	if !errors.Is(err, ErrEventConflict) {
		t.Fatalf("error = %v, want ErrEventConflict", err)
	}
	if next.insertCalls != 1 {
		t.Fatalf("calls = %d, want 1", next.insertCalls)
	}
}

func TestPostgresRetryClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "connection", err: &pgconn.PgError{Code: "08006"}, want: true},
		{name: "serialization", err: &pgconn.PgError{Code: "40001"}, want: true},
		{name: "resources", err: &pgconn.PgError{Code: "53300"}, want: true},
		{name: "unique violation", err: &pgconn.PgError{Code: "23505"}, want: false},
		{name: "conflict", err: ErrEventConflict, want: false},
		{name: "canceled", err: context.Canceled, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryablePostgres(tt.err); got != tt.want {
				t.Fatalf("isRetryablePostgres(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
