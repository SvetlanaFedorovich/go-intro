package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoEventuallySucceeds(t *testing.T) {
	attempts := 0
	retries := 0
	err := Do(context.Background(), Policy{
		MaxAttempts: 4,
		BaseDelay:   time.Millisecond,
		MaxDelay:    2 * time.Millisecond,
		OnRetry: func(_ int, delay time.Duration, _ error) {
			retries++
			if delay < 0 || delay > 2*time.Millisecond {
				t.Errorf("delay = %s", delay)
			}
		},
	}, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if attempts != 3 || retries != 2 {
		t.Fatalf("attempts=%d retries=%d, want 3/2", attempts, retries)
	}
}

func TestDoStopsAtMaxAttempts(t *testing.T) {
	wantErr := errors.New("still unavailable")
	attempts := 0
	err := Do(context.Background(), Policy{
		MaxAttempts: 3,
		BaseDelay:   time.Microsecond,
		MaxDelay:    time.Microsecond,
	}, func() error {
		attempts++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestDoHonorsRetryable(t *testing.T) {
	wantErr := errors.New("permanent")
	attempts := 0
	err := Do(context.Background(), Policy{
		MaxAttempts: 5,
		Retryable:   func(error) bool { return false },
	}, func() error {
		attempts++
		return wantErr
	})
	if !errors.Is(err, wantErr) || attempts != 1 {
		t.Fatalf("error=%v attempts=%d", err, attempts)
	}
}

func TestDoStopsDuringBackoffWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := Do(ctx, Policy{
		MaxAttempts: 5,
		BaseDelay:   time.Hour,
		OnRetry: func(_ int, _ time.Duration, _ error) {
			cancel()
		},
	}, func() error {
		return errors.New("temporary")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
}

func TestParsePolicy(t *testing.T) {
	policy, err := ParsePolicy(4, "25ms", "2s")
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	if policy.MaxAttempts != 4 || policy.BaseDelay != 25*time.Millisecond || policy.MaxDelay != 2*time.Second {
		t.Fatalf("policy = %+v", policy)
	}
	for _, test := range []struct {
		attempts int
		base     string
		max      string
	}{
		{0, "1ms", "1s"},
		{1, "bad", "1s"},
		{1, "1s", "1ms"},
	} {
		if _, err := ParsePolicy(test.attempts, test.base, test.max); err == nil {
			t.Fatalf("ParsePolicy(%d, %q, %q) expected error", test.attempts, test.base, test.max)
		}
	}
}
