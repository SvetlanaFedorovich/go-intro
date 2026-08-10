// Package health предоставляет HTTP-пробы для оркестратора (Kubernetes и т.п.):
//
//	/healthz - liveness: процесс жив и способен отвечать. Без проверки зависимостей.
//	/readyz  - readiness: все зависимости (Kafka, БД) доступны, можно принимать трафик.
//
// Разделение важно: если БД временно недоступна, под НЕ нужно перезапускать
// (liveness ok), но и трафик на него слать не стоит (readiness fail).
package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// Checker - именованная проверка зависимости. Возвращает nil, если зависимость готова.
type Checker struct {
	Name  string
	Check func(ctx context.Context) error
}

// Handler для liveness/readiness пробы.
type Handler struct {
	checkers []Checker
	timeout  time.Duration
	log      *slog.Logger
}

// New создаёт health-хендлер с набором проверок для readiness.
// timeout ограничивает суммарное время всех проверок в одном /readyz запросе.
func New(log *slog.Logger, timeout time.Duration, checkers ...Checker) *Handler {
	if log == nil {
		log = slog.Default()
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Handler{checkers: checkers, timeout: timeout, log: log}
}

// Register для /healthz и /readyz на переданный mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", h.handleLive)
	mux.HandleFunc("/readyz", h.handleReady)
}

// handleLive - liveness. Всегда 200, пока процесс отвечает.
func (h *Handler) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady - readiness. Прогоняет все проверки; при любой ошибке - 503.
func (h *Handler) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	results := make(map[string]string, len(h.checkers))
	ready := true

	for _, c := range h.checkers {
		if err := c.Check(ctx); err != nil {
			ready = false
			results[c.Name] = "error: " + err.Error()
			h.log.Warn("readiness check failed", "dependency", c.Name, "error", err)
			continue
		}
		results[c.Name] = "ok"
	}

	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{
		"ready":  ready,
		"checks": results,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
