package model

import (
	"errors"
	"testing"
)

func TestDataValidate(t *testing.T) {
	tests := []struct {
		name    string
		data    Data
		wantErr error
	}{
		{
			name: "ok",
			data: Data{User: "Max", Age: 31, Email: "max@mail.com"},
		},
		{
			name: "trims spaces",
			data: Data{User: "  Max  ", Age: 1, Email: "  max@mail.com  "},
		},
		{
			name:    "empty user",
			data:    Data{User: "  ", Age: 31, Email: "max@mail.com"},
			wantErr: ErrUserRequired,
		},
		{
			name:    "age zero",
			data:    Data{User: "Max", Age: 0, Email: "max@mail.com"},
			wantErr: ErrAgeInvalid,
		},
		{
			name:    "age negative",
			data:    Data{User: "Max", Age: -1, Email: "max@mail.com"},
			wantErr: ErrAgeInvalid,
		},
		{
			name:    "age overflows int2",
			data:    Data{User: "Max", Age: 32768, Email: "max@mail.com"},
			wantErr: ErrAgeInvalid,
		},
		{
			name: "age at int2 max",
			data: Data{User: "Max", Age: 32767, Email: "max@mail.com"},
		},
		{
			name:    "empty email",
			data:    Data{User: "Max", Age: 31, Email: ""},
			wantErr: ErrEmailRequired,
		},
		{
			name:    "invalid email",
			data:    Data{User: "Max", Age: 31, Email: "not-an-email"},
			wantErr: ErrEmailInvalid,
		},
		{
			name:    "email without domain dot",
			data:    Data{User: "Max", Age: 31, Email: "max@mail"},
			wantErr: ErrEmailInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.data.Validate()
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// Corner-кейсы разбора email: @ в начале/конце, отсутствие точки в домене,
// точка в самом конце, минимально валидный адрес.
func TestIsValidEmail(t *testing.T) {
	tests := []struct {
		email string
		want  bool
	}{
		{"a@b.c", true},        // минимально валидный
		{"max@mail.com", true}, // обычный
		{"@mail.com", false},   // @ в начале - нет local-part
		{"max@", false},        // @ в конце - нет домена
		{"maxmail.com", false}, // нет @
		{"max@mailcom", false}, // домен без точки
		{"max@mail.", false},   // точка в конце домена
		{"max@.com", false},    // точка в начале домена (dot>0 не выполняется)
		{"", false},            // пусто
		{"@", false},           // только @
		{"a@b@c.com", true},    // первый @ считается разделителем, домен "b@c.com" валиден
	}

	for _, tt := range tests {
		t.Run(tt.email, func(t *testing.T) {
			if got := isValidEmail(tt.email); got != tt.want {
				t.Fatalf("isValidEmail(%q) = %v, want %v", tt.email, got, tt.want)
			}
		})
	}
}

// Normalize должен обрезать пробелы, но не менять age.
func TestDataNormalize(t *testing.T) {
	d := Data{User: "  Max  ", Age: 31, Email: "\tmax@mail.com\n"}
	d.Normalize()

	if d.User != "Max" {
		t.Fatalf("user = %q, want %q", d.User, "Max")
	}
	if d.Email != "max@mail.com" {
		t.Fatalf("email = %q, want trimmed", d.Email)
	}
	if d.Age != 31 {
		t.Fatalf("age changed to %d", d.Age)
	}
}
