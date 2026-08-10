package retry

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"time"
)

const (
	DefaultMaxAttempts = 5
	DefaultBaseDelay   = 50 * time.Millisecond
	DefaultMaxDelay    = time.Second
)

type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Retryable   func(error) bool
	OnRetry     func(attempt int, delay time.Duration, err error)
}

func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: DefaultMaxAttempts,
		BaseDelay:   DefaultBaseDelay,
		MaxDelay:    DefaultMaxDelay,
	}
}

func ParsePolicy(maxAttempts int, baseDelay, maxDelay string) (Policy, error) {
	if maxAttempts <= 0 {
		return Policy{}, fmt.Errorf("retry max attempts must be positive, got %d", maxAttempts)
	}
	base, err := time.ParseDuration(baseDelay)
	if err != nil || base <= 0 {
		return Policy{}, fmt.Errorf("retry base delay must be a positive duration, got %q", baseDelay)
	}
	maximum, err := time.ParseDuration(maxDelay)
	if err != nil || maximum <= 0 {
		return Policy{}, fmt.Errorf("retry max delay must be a positive duration, got %q", maxDelay)
	}
	if maximum < base {
		return Policy{}, fmt.Errorf("retry max delay %s must be at least base delay %s", maximum, base)
	}
	return Policy{MaxAttempts: maxAttempts, BaseDelay: base, MaxDelay: maximum}, nil
}

func Do(ctx context.Context, policy Policy, operation func() error) error {
	policy = normalized(policy)
	var lastErr error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = operation()
		if lastErr == nil {
			return nil
		}
		if attempt == policy.MaxAttempts || !canRetry(policy, lastErr) {
			return lastErr
		}

		delay := fullJitter(backoffCap(policy, attempt))
		if policy.OnRetry != nil {
			policy.OnRetry(attempt, delay, lastErr)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func normalized(policy Policy) Policy {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = DefaultMaxAttempts
	}
	if policy.BaseDelay <= 0 {
		policy.BaseDelay = DefaultBaseDelay
	}
	if policy.MaxDelay <= 0 {
		policy.MaxDelay = DefaultMaxDelay
	}
	if policy.MaxDelay < policy.BaseDelay {
		policy.MaxDelay = policy.BaseDelay
	}
	return policy
}

func canRetry(policy Policy, err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return policy.Retryable == nil || policy.Retryable(err)
}

func backoffCap(policy Policy, attempt int) time.Duration {
	delay := policy.BaseDelay
	for current := 1; current < attempt; current++ {
		if delay >= policy.MaxDelay/2 {
			return policy.MaxDelay
		}
		delay *= 2
	}
	if delay > policy.MaxDelay {
		return policy.MaxDelay
	}
	return delay
}

func fullJitter(maxDelay time.Duration) time.Duration {
	if maxDelay <= 0 {
		return 0
	}
	value, err := rand.Int(rand.Reader, big.NewInt(int64(maxDelay)+1))
	if err != nil {
		return maxDelay / 2
	}
	return time.Duration(value.Int64())
}
