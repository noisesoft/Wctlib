package core

import "time"

type ioLimiter struct {
	rate      int64
	capacity  int64
	tokens    float64
	lastCheck time.Time
}

func newIOLimiter(rate int64, burst int64) *ioLimiter {
	if rate <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = 1
	}
	return &ioLimiter{
		rate:      rate,
		capacity:  burst,
		tokens:    float64(burst),
		lastCheck: time.Now(),
	}
}

func (l *ioLimiter) take(bytes int) time.Duration {
	if l == nil || bytes <= 0 {
		return 0
	}
	if l.rate <= 0 {
		return 0
	}

	now := time.Now()
	if l.lastCheck.IsZero() {
		l.lastCheck = now
		l.tokens = float64(l.capacity)
		return 0
	}

	elapsed := now.Sub(l.lastCheck).Seconds()
	l.lastCheck = now
	l.tokens += elapsed * float64(l.rate)
	if l.tokens > float64(l.capacity) {
		l.tokens = float64(l.capacity)
	}

	if float64(bytes) <= l.tokens {
		l.tokens -= float64(bytes)
		return 0
	}

	need := float64(bytes) - l.tokens
	l.tokens = 0
	wait := time.Duration((need / float64(l.rate)) * float64(time.Second))
	return wait
}

func (l *ioLimiter) mustWait(bytes int) time.Duration {
	if l == nil {
		return 0
	}
	return l.take(bytes)
}
