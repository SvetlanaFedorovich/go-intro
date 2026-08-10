package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddr             string
	HealthAddr           string
	KafkaBrokers         []string
	KafkaTopic           string
	KafkaDLQTopic        string
	KafkaGroup           string
	PostgresDSN          string
	StoreDriver          string
	EventRetention       string
	EventCleanupInterval string
	OTLPEndpoint         string
	TraceSampleRatio     float64
	RetryMaxAttempts     int
	RetryBaseDelay       string
	RetryMaxDelay        string
}

func Load() Config {
	return Config{
		HTTPAddr:             getEnv("HTTP_ADDR", ":8080"),
		HealthAddr:           getEnv("HEALTH_ADDR", ":8081"), // порт health-проб worker
		KafkaBrokers:         splitCSV(getEnv("KAFKA_BROKERS", "localhost:9892")),
		KafkaTopic:           getEnv("KAFKA_TOPIC", "data"),
		KafkaDLQTopic:        getEnv("KAFKA_DLQ_TOPIC", "data.dlq"),
		KafkaGroup:           getEnv("KAFKA_GROUP", "go-intro-worker"),
		PostgresDSN:          getEnv("POSTGRES_DSN", "postgres://postgres:pass@localhost:5432/test?sslmode=disable"),
		StoreDriver:          getEnv("STORE_DRIVER", "pgx"), // pgx | gorm
		EventRetention:       getEnv("EVENT_RETENTION", "720h"),
		EventCleanupInterval: getEnv("EVENT_CLEANUP_INTERVAL", "1h"),
		OTLPEndpoint:         getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
		TraceSampleRatio:     getFloat("OTEL_TRACE_SAMPLE_RATIO", 1),
		RetryMaxAttempts:     getInt("RETRY_MAX_ATTEMPTS", 5),
		RetryBaseDelay:       getEnv("RETRY_BASE_DELAY", "50ms"),
		RetryMaxDelay:        getEnv("RETRY_MAX_DELAY", "1s"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func getFloat(key string, fallback float64) float64 {
	value := getEnv(key, "")
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func getInt(key string, fallback int) int {
	value := getEnv(key, "")
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
