package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AntonYurchenko/go-intro/internal/model"
)

func payloadHash(d model.Data) (string, error) {
	payload, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
