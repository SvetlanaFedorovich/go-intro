package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLiveness_AlwaysOK(t *testing.T) {
	h := New(nil, 0)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
}

func TestReadiness_AllOK(t *testing.T) {
	h := New(nil, 0,
		Checker{Name: "db", Check: func(context.Context) error { return nil }},
		Checker{Name: "kafka", Check: func(context.Context) error { return nil }},
	)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}

	var body struct {
		Ready  bool              `json:"ready"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("json: %v", err)
	}
	if !body.Ready {
		t.Fatalf("ready = false, want true")
	}
	if body.Checks["db"] != "ok" || body.Checks["kafka"] != "ok" {
		t.Fatalf("checks = %#v", body.Checks)
	}
}

func TestReadiness_OneFails(t *testing.T) {
	h := New(nil, 0,
		Checker{Name: "db", Check: func(context.Context) error { return nil }},
		Checker{Name: "kafka", Check: func(context.Context) error { return errors.New("broker down") }},
	)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}

	var body struct {
		Ready  bool              `json:"ready"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("json: %v", err)
	}
	if body.Ready {
		t.Fatalf("ready = true, want false")
	}
	if body.Checks["db"] != "ok" {
		t.Fatalf("db check = %q, want ok", body.Checks["db"])
	}
	if body.Checks["kafka"] == "ok" {
		t.Fatalf("kafka check should report error, got %q", body.Checks["kafka"])
	}
}
