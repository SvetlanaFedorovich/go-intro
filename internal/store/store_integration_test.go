//go:build integration

package store

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AntonYurchenko/go-intro/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMigrationCreatesFinalSchema(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	resetIntegrationSchema(t, dsn)
	t.Cleanup(func() { resetIntegrationSchema(t, dsn) })

	assertRegclass(t, dsn, `public."data"`, true)
	assertRegclass(t, dsn, "public.data_event_id_key", true)
	assertRegclass(t, dsn, "public.processed_events", true)
	assertRegclass(t, dsn, "public.processed_kafka", false)
	assertRegclass(t, dsn, "public.processed_events_processed_at_idx", true)

	if got := integrationText(t, dsn, `
		SELECT data_type
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'data' AND column_name = 'age'`); got != "smallint" {
		t.Fatalf("data.age type = %q, want smallint", got)
	}
	if got := integrationText(t, dsn, `
		SELECT data_type
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'data' AND column_name = 'event_id'`); got != "text" {
		t.Fatalf("data.event_id type = %q, want text", got)
	}
	if got := integrationText(t, dsn, `
		SELECT data_type
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'processed_events' AND column_name = 'processed_at'`); got != "timestamp with time zone" {
		t.Fatalf("processed_events.processed_at type = %q", got)
	}
}

func TestStoreImplementationsIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}

	for _, driver := range []string{DriverPGX, DriverGORM} {
		t.Run(driver, func(t *testing.T) {
			resetIntegrationSchema(t, dsn)
			t.Cleanup(func() { resetIntegrationSchema(t, dsn) })

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			db, err := New(ctx, driver, dsn)
			if err != nil {
				t.Fatalf("new store: %v", err)
			}
			defer db.Close()

			if err := db.Ping(ctx); err != nil {
				t.Fatalf("ping: %v", err)
			}
			testInsertOnce(t, ctx, dsn, db)
			testConcurrentInsertOnce(t, ctx, dsn, db)
			testCleanupProcessed(t, ctx, dsn, db)
		})
	}
}

func testInsertOnce(t *testing.T, ctx context.Context, dsn string, db DataStore) {
	t.Helper()
	data := model.Data{User: "Max", Age: 31, Email: "max@example.com"}

	inserted, err := db.InsertOnce(ctx, "event-1", "data", 0, 1, data)
	if err != nil {
		t.Fatalf("first InsertOnce: %v", err)
	}
	if !inserted {
		t.Fatal("first InsertOnce inserted=false")
	}

	inserted, err = db.InsertOnce(ctx, "event-1", "data", 0, 2, data)
	if err != nil {
		t.Fatalf("duplicate InsertOnce: %v", err)
	}
	if inserted {
		t.Fatal("duplicate InsertOnce inserted=true")
	}

	conflicting := data
	conflicting.Email = "other@example.com"
	if _, err := db.InsertOnce(ctx, "event-1", "data", 0, 3, conflicting); !errors.Is(err, ErrEventConflict) {
		t.Fatalf("conflict error = %v, want ErrEventConflict", err)
	}

	if got := integrationCount(t, dsn, `SELECT count(*) FROM public."data" WHERE email = $1`, data.Email); got != 1 {
		t.Fatalf("data rows = %d, want 1", got)
	}
	if got := integrationCount(t, dsn, `SELECT count(*) FROM public.processed_events WHERE event_id = $1`, "event-1"); got != 1 {
		t.Fatalf("processed rows = %d, want 1", got)
	}
}

func testConcurrentInsertOnce(t *testing.T, ctx context.Context, dsn string, db DataStore) {
	t.Helper()
	const attempts = 32
	data := model.Data{User: "Concurrent", Age: 29, Email: "concurrent@example.com"}
	var insertedCount atomic.Int64
	errCh := make(chan error, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			inserted, err := db.InsertOnce(ctx, "event-concurrent", "data", 0, int64(offset), data)
			if err != nil {
				errCh <- err
				return
			}
			if inserted {
				insertedCount.Add(1)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent InsertOnce: %v", err)
	}
	if got := insertedCount.Load(); got != 1 {
		t.Fatalf("inserted count = %d, want 1", got)
	}
	if got := integrationCount(t, dsn, `SELECT count(*) FROM public."data" WHERE email = $1`, data.Email); got != 1 {
		t.Fatalf("concurrent data rows = %d, want 1", got)
	}
}

func testCleanupProcessed(t *testing.T, ctx context.Context, dsn string, db DataStore) {
	t.Helper()
	data := model.Data{User: "Cleanup", Age: 40, Email: "cleanup@example.com"}
	if _, err := db.InsertOnce(ctx, "event-old", "data", 0, 100, data); err != nil {
		t.Fatalf("insert old event: %v", err)
	}
	execIntegration(t, dsn, `
		UPDATE public.processed_events
		SET processed_at = now() - interval '48 hours'
		WHERE event_id = 'event-old'`)

	deleted, err := db.CleanupProcessed(ctx, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	if got := integrationCount(t, dsn, `SELECT count(*) FROM public.processed_events WHERE event_id = $1`, "event-old"); got != 0 {
		t.Fatalf("old processed rows = %d, want 0", got)
	}

	inserted, err := db.InsertOnce(ctx, "event-old", "data", 0, 101, data)
	if err != nil {
		t.Fatalf("replay after cleanup: %v", err)
	}
	if inserted {
		t.Fatal("replay after cleanup inserted=true, want false")
	}
	if got := integrationCount(t, dsn, `SELECT count(*) FROM public."data" WHERE event_id = $1`, "event-old"); got != 1 {
		t.Fatalf("replayed data rows = %d, want 1", got)
	}
	if got := integrationCount(t, dsn, `SELECT count(*) FROM public.processed_events WHERE event_id = $1`, "event-old"); got != 1 {
		t.Fatalf("recreated processed rows = %d, want 1", got)
	}

	conflicting := data
	conflicting.Email = "cleanup-conflict@example.com"
	if _, err := db.InsertOnce(ctx, "event-old", "data", 0, 102, conflicting); !errors.Is(err, ErrEventConflict) {
		t.Fatalf("replay conflict error = %v, want ErrEventConflict", err)
	}
}

func resetIntegrationSchema(t *testing.T, dsn string) {
	t.Helper()
	execIntegration(t, dsn, `
		DROP TABLE IF EXISTS public.processed_events;
		DROP TABLE IF EXISTS public.processed_kafka;
		DROP TABLE IF EXISTS public."data";`)
	schema, err := os.ReadFile("../../migrations/0001_up_schema.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	execIntegration(t, dsn, string(schema))
}

func execIntegration(t *testing.T, dsn, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect integration postgres: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, query, args...); err != nil {
		t.Fatalf("exec integration query: %v", err)
	}
}

func integrationCount(t *testing.T, dsn, query string, args ...any) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect integration postgres: %v", err)
	}
	defer pool.Close()
	var count int
	if err := pool.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		t.Fatalf("count integration rows: %v", err)
	}
	return count
}

func integrationText(t *testing.T, dsn, query string, args ...any) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect integration postgres: %v", err)
	}
	defer pool.Close()
	var value string
	if err := pool.QueryRow(ctx, query, args...).Scan(&value); err != nil {
		t.Fatalf("read integration text: %v", err)
	}
	return value
}

func assertRegclass(t *testing.T, dsn, name string, want bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect integration postgres: %v", err)
	}
	defer pool.Close()
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&exists); err != nil {
		t.Fatalf("read regclass %s: %v", name, err)
	}
	if exists != want {
		t.Fatalf("regclass %s exists=%v, want %v", name, exists, want)
	}
}
