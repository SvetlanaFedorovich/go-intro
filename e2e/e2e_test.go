//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestHTTPKafkaWorkerPostgres(t *testing.T) {
	baseURL := envOrSkip(t, "E2E_BASE_URL")
	dsn := envOrSkip(t, "TEST_POSTGRES_DSN")
	unique := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	email := unique + "@example.com"
	idempotencyKey := unique + "-key"

	response := postData(t, baseURL, idempotencyKey, "e2e-request-1", email)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("first status = %d, body=%s", response.StatusCode, response.Body)
	}
	if response.EventID == "" {
		t.Fatal("first event_id is empty")
	}
	if response.RequestID != "e2e-request-1" {
		t.Fatalf("first request ID = %q", response.RequestID)
	}

	repeated := postData(t, baseURL, idempotencyKey, "e2e-request-2", email)
	if repeated.StatusCode != http.StatusAccepted {
		t.Fatalf("repeated status = %d, body=%s", repeated.StatusCode, repeated.Body)
	}
	if repeated.EventID != response.EventID {
		t.Fatalf("repeated event_id = %q, want %q", repeated.EventID, response.EventID)
	}

	pool := openPostgres(t, dsn)
	defer pool.Close()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, `DELETE FROM public."data" WHERE email = $1`, email)
		_, _ = pool.Exec(ctx, `DELETE FROM public.processed_events WHERE event_id = $1`, response.EventID)
	}()

	waitForCount(t, pool, `SELECT count(*) FROM public."data" WHERE event_id = $1`, response.EventID, 1)
	waitForCount(t, pool, `SELECT count(*) FROM public.processed_events WHERE event_id = $1`, response.EventID, 1)

	// Повтор с тем же Idempotency-Key не должен создать вторую строку.
	time.Sleep(500 * time.Millisecond)
	assertCount(t, pool, `SELECT count(*) FROM public."data" WHERE event_id = $1`, response.EventID, 1)
}

func TestInvalidHTTPPayloadDoesNotReachKafka(t *testing.T) {
	baseURL := envOrSkip(t, "E2E_BASE_URL")
	request, err := http.NewRequest(
		http.MethodPost,
		baseURL+"/data",
		bytes.NewBufferString(`{"user":"","age":0,"email":"broken"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("POST invalid data: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("status = %d, body=%s", response.StatusCode, body)
	}
}

type apiResponse struct {
	StatusCode int
	EventID    string
	RequestID  string
	Body       string
}

func postData(t *testing.T, baseURL, idempotencyKey, requestID, email string) apiResponse {
	t.Helper()
	payload := fmt.Sprintf(`{"user":"E2E","age":31,"email":%q}`, email)
	request, err := http.NewRequest(http.MethodPost, baseURL+"/data", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	request.Header.Set("X-Request-ID", requestID)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("POST data: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var decoded struct {
		EventID string `json:"event_id"`
	}
	if response.StatusCode == http.StatusAccepted {
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}
	return apiResponse{
		StatusCode: response.StatusCode,
		EventID:    decoded.EventID,
		RequestID:  response.Header.Get("X-Request-ID"),
		Body:       string(body),
	}
}

func openPostgres(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping postgres: %v", err)
	}
	return pool
}

func waitForCount(t *testing.T, pool *pgxpool.Pool, query string, arg any, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if count(t, pool, query, arg) == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	assertCount(t, pool, query, arg, want)
}

func assertCount(t *testing.T, pool *pgxpool.Pool, query string, arg any, want int) {
	t.Helper()
	if got := count(t, pool, query, arg); got != want {
		t.Fatalf("count = %d, want %d", got, want)
	}
}

func count(t *testing.T, pool *pgxpool.Pool, query string, arg any) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var value int
	if err := pool.QueryRow(ctx, query, arg).Scan(&value); err != nil {
		t.Fatalf("query count: %v", err)
	}
	return value
}

func envOrSkip(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Skip(name + " is not set")
	}
	return value
}
