package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AntonYurchenko/go-intro/internal/observability"
)

type mockPublisher struct {
	err     error
	calls   int
	key     []byte
	last    []byte
	headers map[string]string
}

func (m *mockPublisher) PublishKeyed(_ context.Context, key, value []byte, headers map[string]string) error {
	m.calls++
	m.key = append([]byte(nil), key...)
	m.last = append([]byte(nil), value...)
	m.headers = maps.Clone(headers)
	return m.err
}

func TestHandleData_Success(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	body := `{"user":"Max","age":31,"email":"max@mail.com"}`
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rr.Code, http.StatusAccepted, rr.Body.String())
	}
	if pub.calls != 1 {
		t.Fatalf("publish calls = %d, want 1", pub.calls)
	}

	var got map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("response json: %v", err)
	}
	if got["status"] != "accepted" {
		t.Fatalf("status field = %q", got["status"])
	}
	if got["event_id"] == "" {
		t.Fatal("event_id is empty")
	}
	if string(pub.key) != got["event_id"] {
		t.Fatalf("Kafka key = %q, event_id = %q", pub.key, got["event_id"])
	}

	var event struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(pub.last, &event); err != nil {
		t.Fatalf("published event: %v", err)
	}
	if event.EventID != got["event_id"] {
		t.Fatalf("published event_id = %q, response event_id = %q", event.EventID, got["event_id"])
	}
}

func TestHandleData_ValidationError(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	body := `{"user":"Max","age":0,"email":"max@mail.com"}`
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	if pub.calls != 0 {
		t.Fatalf("publish should not be called")
	}
}

func TestHandleData_InvalidJSON(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(`{`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestHandleData_MethodNotAllowed(t *testing.T) {
	h := NewHandler(&mockPublisher{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/data", nil)
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleData_UnsupportedMediaType(t *testing.T) {
	h := NewHandler(&mockPublisher{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "text/plain")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusUnsupportedMediaType)
	}
}

func TestHandleData_PublishError(t *testing.T) {
	pub := &mockPublisher{err: errors.New("kafka down")}
	h := NewHandler(pub, nil)

	body := `{"user":"Max","age":31,"email":"max@mail.com"}`
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
}

// Content-Type с параметром charset должен приниматься после mime.ParseMediaType.
func TestHandleData_ContentTypeWithCharset(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	body := `{"user":"Max","age":31,"email":"max@mail.com"}`
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rr.Code, http.StatusAccepted, rr.Body.String())
	}
}

// По контракту Content-Type обязателен.
func TestHandleData_EmptyContentType(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	body := `{"user":"Max","age":31,"email":"max@mail.com"}`
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(body))
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusUnsupportedMediaType)
	}
	if pub.calls != 0 {
		t.Fatalf("publish should not be called without Content-Type")
	}
}

// Лишние поля в JSON должны отклоняться (dec.DisallowUnknownFields).
func TestHandleData_UnknownField(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	body := `{"user":"Max","age":31,"email":"max@mail.com","admin":true}`
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	if pub.calls != 0 {
		t.Fatalf("publish should not be called on unknown field")
	}
}

// Пустое тело запроса - невалидный JSON, 400.
func TestHandleData_EmptyBody(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	req := httptest.NewRequest(http.MethodPost, "/data", http.NoBody)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

// В Kafka должен уходить нормализованный (обрезанный) payload, а не сырой ввод.
func TestHandleData_PublishesNormalizedPayload(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	body := `{"user":"  Max  ","age":31,"email":"  max@mail.com  "}`
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusAccepted)
	}

	var got map[string]any
	if err := json.Unmarshal(pub.last, &got); err != nil {
		t.Fatalf("published payload not json: %v", err)
	}
	if got["user"] != "Max" {
		t.Fatalf("published user = %q, want trimmed %q", got["user"], "Max")
	}
	if got["email"] != "max@mail.com" {
		t.Fatalf("published email = %q, want trimmed", got["email"])
	}
}

// Неизвестный путь - 404, publish не вызывается.
func TestHandleData_UnknownPath(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	req := httptest.NewRequest(http.MethodPost, "/unknown", bytes.NewBufferString(``))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
	if pub.calls != 0 {
		t.Fatalf("publish should not be called on unknown path")
	}
}

func TestHandleData_RejectsJSONLikeMediaType(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(
		`{"user":"Max","age":31,"email":"max@mail.com"}`,
	))
	req.Header.Set("Content-Type", "application/jsonp")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusUnsupportedMediaType)
	}
	if pub.calls != 0 {
		t.Fatalf("publish should not be called for invalid media type")
	}
}

func TestHandleData_RejectsMultipleJSONValues(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	body := `{"user":"Max","age":31,"email":"max@mail.com"} {}`
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	if pub.calls != 0 {
		t.Fatalf("publish should not be called for multiple JSON values")
	}
}

func TestHandleData_AllowsTrailingWhitespace(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	body := "{\"user\":\"Max\",\"age\":31,\"email\":\"max@mail.com\"}\n\t "
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rr.Code, http.StatusAccepted, rr.Body.String())
	}
	if pub.calls != 1 {
		t.Fatalf("publish calls = %d, want 1", pub.calls)
	}
}

func TestHandleData_RejectsOversizedBody(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)

	body := `{"user":"` + strings.Repeat("x", maxRequestBodyBytes) + `","age":31,"email":"max@mail.com"}`
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", rr.Code, http.StatusRequestEntityTooLarge, rr.Body.String())
	}
	if pub.calls != 0 {
		t.Fatalf("publish should not be called for oversized body")
	}
}

func TestHandleData_MethodNotAllowedSetsAllowHeader(t *testing.T) {
	h := NewHandler(&mockPublisher{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/data", nil)
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if got := rr.Header().Get("Allow"); got != http.MethodPost {
		t.Fatalf("Allow = %q, want %q", got, http.MethodPost)
	}
}

func TestHandleData_IdempotencyKeyProducesStableEventID(t *testing.T) {
	send := func() string {
		pub := &mockPublisher{}
		h := NewHandler(pub, nil)
		req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(
			`{"user":"Max","age":31,"email":"max@mail.com"}`,
		))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "request-42")
		rr := httptest.NewRecorder()

		h.Routes().ServeHTTP(rr, req)
		if rr.Code != http.StatusAccepted {
			t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
		}

		var response map[string]string
		if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
			t.Fatalf("response json: %v", err)
		}
		return response["event_id"]
	}

	if first, second := send(), send(); first == "" || first != second {
		t.Fatalf("event ids are not stable: %q != %q", first, second)
	}
}

func TestHandleData_RejectsInvalidIdempotencyKey(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(
		`{"user":"Max","age":31,"email":"max@mail.com"}`,
	))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", strings.Repeat("x", maxIdempotencyKeyBytes+1))
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	if pub.calls != 0 {
		t.Fatal("publish should not be called for invalid Idempotency-Key")
	}
}

func TestHandleData_PropagatesRequestIDToKafka(t *testing.T) {
	pub := &mockPublisher{}
	h := NewHandler(pub, nil)
	req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewBufferString(
		`{"user":"Max","age":31,"email":"max@mail.com"}`,
	))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(observability.ContextWithRequestID(req.Context(), "request-42"))
	rr := httptest.NewRecorder()

	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	if got := pub.headers["x-request-id"]; got != "request-42" {
		t.Fatalf("Kafka request ID = %q", got)
	}
}
