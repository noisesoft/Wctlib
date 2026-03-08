package adapter

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/telemetry"
	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

var errNoAdapter = errors.New("no adapter accepted request")

type Logger interface {
	Debug(message string, kv ...any)
	Info(message string, kv ...any)
	Warn(message string, kv ...any)
	Error(message string, kv ...any)
}

type Capability struct {
	Receive bool
	Send    bool
	Resume  bool
	MaxSize int
}

type AdapterConfig struct {
	SessionID  string
	StreamID   string
	Direction  types.StreamDirection
	Request    types.StreamRequest
	Profile    types.StreamProfile
	Logger     Logger
	Metrics    telemetry.MetricSink
	Deadline   time.Time
	RetryLimit int
}

type InboundEndpoint interface {
	Frames(ctx context.Context) <-chan types.Frame
	Errors() <-chan error
	Ack(ctx context.Context, seq uint64, offset int64) error
	Close(ctx context.Context) error
}

type OutboundEndpoint interface {
	Send(ctx context.Context, frame *types.Frame) error
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
}

type TransportAdapter interface {
	ID() string
	Version() string
	Priority() int
	Capabilities() Capability
	Supports(ctx context.Context, cfg AdapterConfig) bool
	OpenInbound(ctx context.Context, cfg AdapterConfig) (InboundEndpoint, error)
	OpenOutbound(ctx context.Context, cfg AdapterConfig) (OutboundEndpoint, error)
}

type ProbeResult struct {
	AdapterID string
	Supported bool
	Reason    string
	Score     int
	Capability
}

const (
	maxAdapterScore  = 100
	minAdapterScore  = 0
	successScoreStep = 8
	failScoreStep    = 22
)

type adapterHealth struct {
	failures    int
	success     int
	lastFailure time.Time
	lastSuccess time.Time
	score       int
}

type Registry struct {
	mu        sync.RWMutex
	adapters  []TransportAdapter
	healthTTL time.Duration
	health    map[string]*adapterHealth
}

func NewRegistry() *Registry {
	return &Registry{healthTTL: 60 * time.Second, health: map[string]*adapterHealth{}}
}

func (r *Registry) Register(adapter TransportAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.adapters {
		if r.adapters[i].ID() == adapter.ID() {
			r.adapters[i] = adapter
			return
		}
	}
	r.adapters = append(r.adapters, adapter)
	sort.SliceStable(r.adapters, func(i, j int) bool {
		return r.adapters[i].Priority() > r.adapters[j].Priority()
	})
}

func (r *Registry) Unregister(adapterID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	filtered := r.adapters[:0]
	for _, adapter := range r.adapters {
		if adapter.ID() != adapterID {
			filtered = append(filtered, adapter)
		}
	}
	r.adapters = filtered
	delete(r.health, adapterID)
}

func (r *Registry) list() []TransportAdapter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	clone := make([]TransportAdapter, len(r.adapters))
	copy(clone, r.adapters)
	return clone
}

func (r *Registry) All() []TransportAdapter {
	return r.list()
}

func (r *Registry) ByID(id string) (TransportAdapter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, candidate := range r.adapters {
		if candidate.ID() == id {
			return candidate, true
		}
	}
	return nil, false
}

func (r *Registry) Select(ctx context.Context, cfg AdapterConfig) (TransportAdapter, error) {
	adapters := r.Probe(ctx, cfg)
	if len(adapters) == 0 {
		return nil, errNoAdapter
	}
	for _, item := range adapters {
		if item.Supported {
			for _, candidate := range r.list() {
				if candidate.ID() == item.AdapterID {
					return candidate, nil
				}
			}
		}
	}
	return nil, errNoAdapter
}

func (r *Registry) Probe(ctx context.Context, cfg AdapterConfig) []ProbeResult {
	adapters := r.list()
	results := make([]ProbeResult, 0, len(adapters))
	for _, candidate := range adapters {
		id := candidate.ID()
		if r.inQuarantine(id) {
			cap := candidate.Capabilities()
			results = append(results, ProbeResult{
				AdapterID:  id,
				Supported:  false,
				Reason:     "quarantined",
				Score:      candidate.Priority() + r.healthScore(id),
				Capability: cap,
			})
			continue
		}

		cap := candidate.Capabilities()
		supported := candidate.Supports(ctx, cfg)
		if supported && cap.MaxSize > 0 && cfg.Profile.ChunkSize > cap.MaxSize {
			supported = false
		}
		result := ProbeResult{
			AdapterID:  id,
			Supported:  supported,
			Capability: cap,
			Score:      candidate.Priority() + r.healthScore(id),
		}
		if supported {
			result.Reason = "ok"
		} else {
			if cap.MaxSize > 0 && cfg.Profile.ChunkSize > cap.MaxSize {
				result.Reason = "chunk size exceeds adapter max size"
			} else {
				result.Reason = "not supported"
			}
		}
		results = append(results, result)
	}
	sort.SliceStable(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	return results
}

func (r *Registry) ReportFailure(adapterID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.health[adapterID]
	if !ok {
		h = &adapterHealth{score: maxAdapterScore}
		r.health[adapterID] = h
	}
	h.failures++
	h.lastFailure = time.Now()
	h.success = 0
	h.score -= failScoreStep
	if h.score < minAdapterScore {
		h.score = minAdapterScore
	}
}

func (r *Registry) ReportSuccess(adapterID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.health[adapterID]
	if !ok {
		h = &adapterHealth{score: maxAdapterScore}
		r.health[adapterID] = h
	}
	h.success++
	h.lastSuccess = time.Now()
	h.failures = 0
	h.score += successScoreStep
	if h.score > maxAdapterScore {
		h.score = maxAdapterScore
	}
}

func (r *Registry) inQuarantine(adapterID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.health[adapterID]
	if !ok {
		return false
	}
	if time.Since(h.lastFailure) > r.healthTTL {
		delete(r.health, adapterID)
		return false
	}
	return h.failures >= 3 || h.score < 35
}

func (r *Registry) healthScore(adapterID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.health[adapterID]
	if !ok {
		return maxAdapterScore
	}
	if h.score == 0 {
		return maxAdapterScore
	}
	if h.lastSuccess.IsZero() {
		return h.score
	}
	age := time.Since(h.lastSuccess)
	if age > r.healthTTL {
		h.score += successScoreStep
		if h.score > maxAdapterScore {
			h.score = maxAdapterScore
		}
	}
	return h.score
}
