package core

import (
	"context"
	"math"
	"sync"
	"time"
)

type FlowPolicy struct {
	WindowSize     int
	MaxOutstanding int
	ChunkSize      int
	RetryMax       int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	MaxBytesPerSec int64
	Adaptive       bool
	Ordered        bool
}

type FlowController struct {
	policy         FlowPolicy
	mu             sync.Mutex
	outstanding    int64
	lastRTT        float64
	lastLoss       float64
	adaptiveWindow int
	opQueue        chan struct{}
}

func NewFlowController(policy FlowPolicy) *FlowController {
	window := policy.WindowSize
	if window <= 0 {
		window = 64
	}
	limit := policy.MaxOutstanding
	if limit > 0 && limit < window {
		window = limit
	}
	if window <= 0 {
		window = 1
	}
	return &FlowController{
		policy:         policy,
		adaptiveWindow: window,
		opQueue:        make(chan struct{}, window),
		outstanding:    0,
		lastRTT:        0,
		lastLoss:       0,
	}
}

func (f *FlowController) Acquire(ctx context.Context, need int) error {
	for i := 0; i < need; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f.opQueue <- struct{}{}:
			f.mu.Lock()
			f.outstanding++
			f.mu.Unlock()
		}
	}
	return nil
}

func (f *FlowController) Release(done int) {
	for i := 0; i < done; i++ {
		f.mu.Lock()
		f.outstanding--
		if f.outstanding < 0 {
			f.outstanding = 0
		}
		f.mu.Unlock()
		select {
		case <-f.opQueue:
		default:
		}
	}
}

func (f *FlowController) Active() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.outstanding
}

func (f *FlowController) ObserveFeedback(rtt, loss float64) {
	if !f.policy.Adaptive {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lastRTT == 0 {
		f.lastRTT = rtt
	} else {
		f.lastRTT = 0.7*f.lastRTT + 0.3*rtt
	}
	if f.lastLoss == 0 {
		f.lastLoss = loss
	} else {
		f.lastLoss = 0.7*f.lastLoss + 0.3*loss
	}
	factor := 1.0 - f.lastLoss*3.0
	if factor < 0.4 {
		factor = 0.4
	}
	if factor > 1.6 {
		factor = 1.6
	}
	if f.lastRTT > 80 {
		factor *= 0.9
	}
	if f.lastRTT < 30 {
		factor *= 1.05
	}
	window := float64(f.policy.WindowSize) * factor
	limit := window
	if f.policy.MaxOutstanding > 0 {
		limit = math.Min(float64(f.policy.MaxOutstanding), window)
	}
	window = math.Max(16, math.Min(512, limit))
	f.adaptiveWindow = int(window)
}

func (f *FlowController) WindowSize() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.adaptiveWindow > 0 {
		return f.adaptiveWindow
	}
	if f.policy.WindowSize > 0 {
		return f.policy.WindowSize
	}
	return 64
}

func (f *FlowController) MaxOutstanding() int {
	if f.policy.MaxOutstanding > 0 {
		return f.policy.MaxOutstanding
	}
	return f.WindowSize() * 2
}
