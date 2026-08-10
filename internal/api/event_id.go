package api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

const maxIdempotencyKeyBytes = 128

var errInvalidIdempotencyKey = errors.New("Idempotency-Key must contain 1 to 128 bytes")

func newEventID(rawKey string, headerPresent bool) (string, error) {
	key := strings.TrimSpace(rawKey)
	if headerPresent {
		if key == "" || len(key) > maxIdempotencyKeyBytes {
			return "", errInvalidIdempotencyKey
		}
		// Не сохраняем клиентский ключ в Kafka/БД в открытом виде.
		sum := sha256.Sum256([]byte("go-intro:idempotency:" + key))
		return hex.EncodeToString(sum[:]), nil
	}

	var id [32]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}
