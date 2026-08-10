package store

import (
	"context"
	"strings"
	"testing"
)

func TestNewRejectsUnknownDriver(t *testing.T) {
	_, err := New(context.Background(), "mongo", "unused")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "unknown store driver") {
		t.Fatalf("error = %q", err)
	}
}
