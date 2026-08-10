package config

import (
	"reflect"
	"testing"
)

func TestGetEnv(t *testing.T) {
	t.Run("returns fallback when unset", func(t *testing.T) {
		// переменная не задана - ожидаем fallback
		if got := getEnv("CONFIG_TEST_UNSET", "def"); got != "def" {
			t.Fatalf("got %q, want %q", got, "def")
		}
	})

	t.Run("returns value when set", func(t *testing.T) {
		t.Setenv("CONFIG_TEST_KEY", "val")
		if got := getEnv("CONFIG_TEST_KEY", "def"); got != "val" {
			t.Fatalf("got %q, want %q", got, "val")
		}
	})

	t.Run("returns fallback when empty", func(t *testing.T) {
		// пустая строка трактуется как «не задано» - важный corner-кейс
		t.Setenv("CONFIG_TEST_EMPTY", "")
		if got := getEnv("CONFIG_TEST_EMPTY", "def"); got != "def" {
			t.Fatalf("got %q, want %q", got, "def")
		}
	})
}

func TestSplitCSV(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"single", "localhost:9092", []string{"localhost:9092"}},
		{"multiple", "a:1,b:2,c:3", []string{"a:1", "b:2", "c:3"}},
		{"trims spaces", " a:1 , b:2 ", []string{"a:1", "b:2"}},
		{"skips empty elements", "a:1,,b:2,", []string{"a:1", "b:2"}},
		{"empty string", "", []string{}},
		{"only commas and spaces", " , , ", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitCSV(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("splitCSV(%q) = %#v, want %#v", tt.in, got, tt.want)
			}
		})
	}
}

func TestLoad_Defaults(t *testing.T) {
	// изолируем от окружения хоста: все ключи чистим
	for _, k := range []string{
		"HTTP_ADDR", "KAFKA_BROKERS", "KAFKA_TOPIC",
		"KAFKA_DLQ_TOPIC", "KAFKA_GROUP", "POSTGRES_DSN", "STORE_DRIVER",
		"EVENT_RETENTION", "EVENT_CLEANUP_INTERVAL",
		"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_TRACE_SAMPLE_RATIO",
		"RETRY_MAX_ATTEMPTS", "RETRY_BASE_DELAY", "RETRY_MAX_DELAY",
	} {
		t.Setenv(k, "")
	}

	cfg := Load()

	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q", cfg.HTTPAddr)
	}
	if !reflect.DeepEqual(cfg.KafkaBrokers, []string{"localhost:9892"}) {
		t.Errorf("KafkaBrokers = %#v", cfg.KafkaBrokers)
	}
	if cfg.KafkaTopic != "data" {
		t.Errorf("KafkaTopic = %q", cfg.KafkaTopic)
	}
	if cfg.KafkaDLQTopic != "data.dlq" {
		t.Errorf("KafkaDLQTopic = %q", cfg.KafkaDLQTopic)
	}
	if cfg.StoreDriver != "pgx" {
		t.Errorf("StoreDriver = %q", cfg.StoreDriver)
	}
	if cfg.EventRetention != "720h" {
		t.Errorf("EventRetention = %q", cfg.EventRetention)
	}
	if cfg.EventCleanupInterval != "1h" {
		t.Errorf("EventCleanupInterval = %q", cfg.EventCleanupInterval)
	}
	if cfg.OTLPEndpoint != "http://localhost:4318" {
		t.Errorf("OTLPEndpoint = %q", cfg.OTLPEndpoint)
	}
	if cfg.TraceSampleRatio != 1 {
		t.Errorf("TraceSampleRatio = %v", cfg.TraceSampleRatio)
	}
	if cfg.RetryMaxAttempts != 5 || cfg.RetryBaseDelay != "50ms" || cfg.RetryMaxDelay != "1s" {
		t.Errorf("retry config = %d/%s/%s", cfg.RetryMaxAttempts, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
	}
}

func TestLoad_Override(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":9999")
	t.Setenv("KAFKA_BROKERS", "k1:9092,k2:9092")
	t.Setenv("STORE_DRIVER", "gorm")
	t.Setenv("EVENT_RETENTION", "720h")
	t.Setenv("EVENT_CLEANUP_INTERVAL", "30m")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://tempo:4318")
	t.Setenv("OTEL_TRACE_SAMPLE_RATIO", "0.25")
	t.Setenv("RETRY_MAX_ATTEMPTS", "7")
	t.Setenv("RETRY_BASE_DELAY", "25ms")
	t.Setenv("RETRY_MAX_DELAY", "3s")

	cfg := Load()

	if cfg.HTTPAddr != ":9999" {
		t.Errorf("HTTPAddr = %q, want :9999", cfg.HTTPAddr)
	}
	if !reflect.DeepEqual(cfg.KafkaBrokers, []string{"k1:9092", "k2:9092"}) {
		t.Errorf("KafkaBrokers = %#v", cfg.KafkaBrokers)
	}
	if cfg.StoreDriver != "gorm" {
		t.Errorf("StoreDriver = %q, want gorm", cfg.StoreDriver)
	}
	if cfg.EventRetention != "720h" {
		t.Errorf("EventRetention = %q, want 720h", cfg.EventRetention)
	}
	if cfg.EventCleanupInterval != "30m" {
		t.Errorf("EventCleanupInterval = %q, want 30m", cfg.EventCleanupInterval)
	}
	if cfg.OTLPEndpoint != "http://tempo:4318" {
		t.Errorf("OTLPEndpoint = %q", cfg.OTLPEndpoint)
	}
	if cfg.TraceSampleRatio != 0.25 {
		t.Errorf("TraceSampleRatio = %v, want 0.25", cfg.TraceSampleRatio)
	}
	if cfg.RetryMaxAttempts != 7 || cfg.RetryBaseDelay != "25ms" || cfg.RetryMaxDelay != "3s" {
		t.Errorf("retry config = %d/%s/%s", cfg.RetryMaxAttempts, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
	}
}
