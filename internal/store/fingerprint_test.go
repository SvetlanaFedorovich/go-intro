package store

import (
	"testing"

	"github.com/AntonYurchenko/go-intro/internal/model"
)

func TestPayloadHash(t *testing.T) {
	data := model.Data{User: "Max", Age: 31, Email: "max@mail.com"}
	first, err := payloadHash(data)
	if err != nil {
		t.Fatalf("first hash: %v", err)
	}
	second, err := payloadHash(data)
	if err != nil {
		t.Fatalf("second hash: %v", err)
	}
	if first != second {
		t.Fatalf("hash is not stable: %q != %q", first, second)
	}

	changed, err := payloadHash(model.Data{User: "Max", Age: 32, Email: "max@mail.com"})
	if err != nil {
		t.Fatalf("changed hash: %v", err)
	}
	if first == changed {
		t.Fatal("different payloads have equal hashes")
	}
}
