package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"

	"github.com/AntonYurchenko/go-intro/internal/model"
	"github.com/AntonYurchenko/go-intro/internal/observability"
)

const maxRequestBodyBytes = 1 << 20 // 1 MiB

type Publisher interface {
	PublishKeyed(ctx context.Context, key, value []byte, headers map[string]string) error
}

type Handler struct {
	publisher Publisher
	log       *slog.Logger
}

func NewHandler(publisher Publisher, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{publisher: publisher, log: logger}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/data", h.handleData)
	return mux
}

func (h *Handler) handleData(w http.ResponseWriter, r *http.Request) {
	logger := observability.Logger(r.Context(), h.log)
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		h.writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		h.writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var data model.Data
	if err := dec.Decode(&data); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			h.writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body is too large"})
			return
		}
		h.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	// Принимаем ровно один JSON-объект. Второй объект или любой мусор после
	// первого не должны молча игнорироваться.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			h.writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body is too large"})
			return
		}
		h.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body must contain a single JSON object"})
		return
	}

	if err := data.Validate(); err != nil {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	idempotencyKeys := r.Header.Values("Idempotency-Key")
	if len(idempotencyKeys) > 1 {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "multiple Idempotency-Key headers are not allowed"})
		return
	}
	rawKey := ""
	if len(idempotencyKeys) == 1 {
		rawKey = idempotencyKeys[0]
	}
	eventID, err := newEventID(rawKey, len(idempotencyKeys) == 1)
	if err != nil {
		if errors.Is(err, errInvalidIdempotencyKey) {
			h.writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		logger.Error("generate event id", "error", err)
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	event := model.Event{EventID: eventID, Data: data}
	payload, err := json.Marshal(event)
	if err != nil {
		logger.Error("marshal payload", "error", err)
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	headers := map[string]string{}
	if requestID := observability.RequestID(r.Context()); requestID != "" {
		headers[strings.ToLower(observability.RequestIDHeader)] = requestID
	}
	observability.InjectTrace(r.Context(), headers)
	if err := h.publisher.PublishKeyed(r.Context(), []byte(eventID), payload, headers); err != nil {
		logger.Error("publish to kafka", "error", err, "event_id", eventID)
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to publish message"})
		return
	}

	h.writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted", "event_id": eventID})
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil && !errors.Is(err, http.ErrHandlerTimeout) {
		h.log.Error("write response", "error", err)
	}
}
