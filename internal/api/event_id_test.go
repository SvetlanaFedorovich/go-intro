package api

import (
	"errors"
	"testing"
)

func TestNewEventID_IdempotencyKeyIsStable(t *testing.T) {
	first, err := newEventID("order-42", true)
	if err != nil {
		t.Fatalf("first id: %v", err)
	}
	second, err := newEventID(" order-42 ", true)
	if err != nil {
		t.Fatalf("second id: %v", err)
	}
	if first != second {
		t.Fatalf("ids differ: %q != %q", first, second)
	}
	if len(first) != 64 {
		t.Fatalf("id length = %d, want 64", len(first))
	}
}

func TestNewEventID_WithoutKeyIsUnique(t *testing.T) {
	first, err := newEventID("", false)
	if err != nil {
		t.Fatalf("first id: %v", err)
	}
	second, err := newEventID("", false)
	if err != nil {
		t.Fatalf("second id: %v", err)
	}
	if first == second {
		t.Fatalf("generated duplicate id %q", first)
	}
}

func TestNewEventID_RejectsInvalidKey(t *testing.T) {
	tests := []string{"", "   ", string(make([]byte, maxIdempotencyKeyBytes+1))}
	for _, key := range tests {
		if _, err := newEventID(key, true); !errors.Is(err, errInvalidIdempotencyKey) {
			t.Fatalf("key length %d: got %v, want invalid key", len(key), err)
		}
	}
}
