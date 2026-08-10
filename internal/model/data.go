package model

import (
	"errors"
	"math"
	"strings"
)

var (
	ErrUserRequired  = errors.New("user is required")
	ErrAgeInvalid    = errors.New("age must be greater than 0 and fit into int2")
	ErrEmailRequired = errors.New("email is required")
	ErrEmailInvalid  = errors.New("email is invalid")
)

// maxAge - верхняя граница возраста. Требование README: "число больше 0" (без явного
// верхнего предела), поэтому ограничиваем только пределом БД: колонка age имеет тип int2.
// Это защищает от переполнения (pgx вернул бы ошибку БД, gorm молча обрезал бы int16()),
// не отклоняя значения, которые спецификация считает валидными.
const maxAge = math.MaxInt16 // 32767

type Data struct {
	User  string `json:"user"`
	Age   int    `json:"age"`
	Email string `json:"email"`
}

// Event - сообщение, передаваемое через Kafka. EventID стабилен для повторных
// HTTP-запросов с одинаковым Idempotency-Key.
type Event struct {
	EventID string `json:"event_id"`
	Data
}

func (d *Data) Normalize() {
	d.User = strings.TrimSpace(d.User)
	d.Email = strings.TrimSpace(d.Email)
}

func (d *Data) Validate() error {
	d.Normalize()

	if d.User == "" {
		return ErrUserRequired
	}
	if d.Age <= 0 || d.Age > maxAge {
		return ErrAgeInvalid
	}
	if d.Email == "" {
		return ErrEmailRequired
	}
	if !isValidEmail(d.Email) {
		return ErrEmailInvalid
	}
	return nil
}

func isValidEmail(email string) bool {
	at := strings.IndexByte(email, '@')
	if at <= 0 || at == len(email)-1 {
		return false
	}
	domain := email[at+1:]
	dot := strings.LastIndexByte(domain, '.')
	return dot > 0 && dot < len(domain)-1
}
