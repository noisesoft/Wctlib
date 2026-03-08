package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/adapter"
	"github.com/noisesoft/Wctlib/pkg/wctlib/buffer"
	"github.com/noisesoft/Wctlib/pkg/wctlib/store"
	"github.com/noisesoft/Wctlib/pkg/wctlib/telemetry"
	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

type Runtime struct {
	mu         sync.RWMutex
	sessions   map[string]*StreamSession
	registry   *adapter.Registry
	store      store.CheckpointStore
	bufferPool *buffer.Pool
	flowPolicy FlowPolicy
	metrics    telemetry.MetricSink
	logger     adapter.Logger
	dataPlane  *Scheduler
	control    *Scheduler
	stop       func()
}

type RuntimeConfig struct {
	Registry       *adapter.Registry
	Store          store.CheckpointStore
	FlowPolicy     FlowPolicy
	Metrics        telemetry.MetricSink
	Logger         adapter.Logger
	DataWorkers    int
	ControlWorkers int
	GCInterval     time.Duration
}

func NewRuntime(cfg RuntimeConfig) (*Runtime, error) {
	if err := store.Validate(cfg.Store); err != nil {
		return nil, err
	}
	if cfg.Registry == nil {
		cfg.Registry = adapter.NewRegistry()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = telemetry.NewNoopMetrics()
	}
	if cfg.DataWorkers <= 0 {
		cfg.DataWorkers = 4
	}
	if cfg.ControlWorkers <= 0 {
		cfg.ControlWorkers = 2
	}
	if cfg.GCInterval <= 0 {
		cfg.GCInterval = 30 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	data := NewScheduler(cfg.DataWorkers)
	control := NewScheduler(cfg.ControlWorkers)
	data.Start(ctx)
	control.Start(ctx)

	rt := &Runtime{
		sessions:   map[string]*StreamSession{},
		registry:   cfg.Registry,
		store:      cfg.Store,
		bufferPool: buffer.NewPool(),
		flowPolicy: cfg.FlowPolicy,
		metrics:    cfg.Metrics,
		logger:     cfg.Logger,
		dataPlane:  data,
		control:    control,
		stop:       cancel,
	}
	go rt.runGC(ctx, cfg.GCInterval)
	return rt, nil
}

func (r *Runtime) RegisterAdapter(adapter adapter.TransportAdapter) {
	r.registry.Register(adapter)
}

func (r *Runtime) Registry() *adapter.Registry {
	return r.registry
}

func (r *Runtime) Stop() {
	if r.stop != nil {
		r.stop()
	}
	r.dataPlane.Stop()
	r.control.Stop()
	r.mu.Lock()
	for _, session := range r.sessions {
		session.Close()
	}
	r.sessions = map[string]*StreamSession{}
	r.mu.Unlock()
}

func (r *Runtime) StartSession(ctx context.Context, req types.StreamRequest) (*StreamSession, error) {
	if req.StreamID == "" {
		return nil, errors.New("stream id is empty")
	}
	if req.Profile.ChunkSize <= 0 {
		req.Profile.ChunkSize = 64 * 1024
	}
	if req.Profile.WindowSize <= 0 {
		req.Profile.WindowSize = 64
	}
	if req.Profile.MaxOutstanding <= 0 {
		req.Profile.MaxOutstanding = 256
	}

	cfg := adapter.AdapterConfig{
		SessionID:  generateSessionID(req.StreamID),
		StreamID:   req.StreamID,
		Direction:  req.Direction,
		Request:    req,
		Profile:    req.Profile,
		Logger:     r.logger,
		Metrics:    r.metrics,
		RetryLimit: req.Profile.RetryMax,
	}

	sessionID := cfg.SessionID
	checkpoint := types.StreamCheckpoint{SessionID: sessionID, StreamID: req.StreamID}
	if req.ResumeToken != "" {
		resume, err := types.ParseResumeToken(req.ResumeToken)
		if err != nil {
			return nil, err
		}
		if resume.StreamID != req.StreamID {
			return nil, fmt.Errorf("resume stream mismatch: token=%s request=%s", resume.StreamID, req.StreamID)
		}
		stored, err := r.store.Load(ctx, resume.SessionID)
		if err != nil {
			if _, ok := err.(store.ErrNotFound); ok {
				r.logger.Warn("checkpoint not found for resume", "session", resume.SessionID)
			} else {
				return nil, err
			}
		} else {
			reqCheckpoint := types.StreamCheckpoint{
				SessionID:  resume.SessionID,
				StreamID:   resume.StreamID,
				LastOffset: resume.Offset,
				LastSeq:    resume.Sequence,
			}
			checkpoint = store.ReconcileCheckpoints(reqCheckpoint, stored)
			sessionID = checkpoint.SessionID
			cfg.SessionID = checkpoint.SessionID
		}
	}

	cfg.SessionID = sessionID
	policy := r.flowPolicy
	if req.Profile.ChunkSize > 0 {
		policy.ChunkSize = req.Profile.ChunkSize
	}
	if req.Profile.WindowSize > 0 {
		policy.WindowSize = req.Profile.WindowSize
	}
	if req.Profile.MaxOutstanding > 0 {
		policy.MaxOutstanding = req.Profile.MaxOutstanding
	}
	if req.Profile.RetryMax > 0 {
		policy.RetryMax = req.Profile.RetryMax
	}
	if req.Profile.RetryBaseDelay > 0 {
		policy.RetryBaseDelay = req.Profile.RetryBaseDelay
	}
	if req.Profile.RetryMaxDelay > 0 {
		policy.RetryMaxDelay = req.Profile.RetryMaxDelay
	}
	if req.Profile.MaxBytesPerSec > 0 {
		policy.MaxBytesPerSec = req.Profile.MaxBytesPerSec
	}
	policy.Adaptive = req.Profile.Adaptive
	policy.Ordered = req.Profile.Ordered

	if req.Direction == types.DirectionSend {
		if req.Payload == nil {
			return nil, fmt.Errorf("payload is required for send direction")
		}
		if checkpoint.LastOffset > 0 {
			seeker, ok := req.Payload.(io.Seeker)
			if !ok {
				return nil, fmt.Errorf("resume requires io.Seeker for seekable source")
			}
			_, err := seeker.Seek(checkpoint.LastOffset, io.SeekStart)
			if err != nil {
				return nil, fmt.Errorf("resume seek failed: %w", err)
			}
		}
	}
	if req.Direction == types.DirectionReceive && req.Sink == nil {
		return nil, fmt.Errorf("sink is required for receive direction")
	}

	adapters, outbound, inbound, err := r.pickAdapter(ctx, cfg)
	if err != nil {
		return nil, err
	}

	flow := NewFlowController(policy)
	session := newStreamSession(
		sessionID,
		req.StreamID,
		req,
		checkpoint,
		policy,
		outbound,
		inbound,
		r.store,
		flow,
		r.bufferPool,
		r.metrics,
		r.logger,
	)
	session.adapterName = adapters.ID()

	ctx, cancel := context.WithCancel(ctx)
	session.cancel = cancel

	r.mu.Lock()
	r.sessions[sessionID] = session
	r.mu.Unlock()

	go func() {
		<-session.doneCh
		r.mu.Lock()
		delete(r.sessions, sessionID)
		r.mu.Unlock()
	}()

	if req.Direction == types.DirectionReceive {
		hasEndpoint := inbound != nil
		if !hasEndpoint {
			session.fail(fmt.Errorf("no inbound endpoint for receive direction"))
			return nil, errors.New("no inbound endpoint for receive direction")
		}
		if !r.control.Submit(func(innerCtx context.Context) error {
			session.runReceive(innerCtx)
			return nil
		}) {
			session.fail(fmt.Errorf("control plane queue full"))
			return nil, errors.New("control plane queue is full")
		}
	} else {
		if !r.dataPlane.Submit(func(innerCtx context.Context) error {
			session.runSend(innerCtx, req.Payload)
			return nil
		}) {
			session.fail(fmt.Errorf("data plane queue full"))
			return nil, errors.New("data plane queue is full")
		}
	}

	r.metrics.LogEvent("stream_started", telemetry.Labels{"stream_id": req.StreamID, "session_id": sessionID, "adapter": session.adapterName})
	return session, nil
}

func (r *Runtime) StopSession(sessionID string) {
	r.mu.Lock()
	session, ok := r.sessions[sessionID]
	r.mu.Unlock()
	if !ok {
		return
	}
	session.Close()
	r.mu.Lock()
	delete(r.sessions, sessionID)
	r.mu.Unlock()
}

func (r *Runtime) GetSession(sessionID string) (*StreamSession, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.sessions[sessionID]
	return session, ok
}

func (r *Runtime) pickAdapter(ctx context.Context, cfg adapter.AdapterConfig) (adapter.TransportAdapter, adapter.OutboundEndpoint, adapter.InboundEndpoint, error) {
	probes := orderProbeCandidates(cfg.Profile.Preferred, r.registry.Probe(ctx, cfg))
	if len(probes) == 0 {
		return nil, nil, nil, fmt.Errorf("no adapters registered")
	}

	var lastErr error
	for _, probe := range probes {
		if !probe.Supported {
			continue
		}
		adapterImpl, ok := r.registry.ByID(probe.AdapterID)
		if !ok {
			continue
		}

		if cfg.Direction == types.DirectionReceive {
			inbound, inErr := adapterImpl.OpenInbound(ctx, cfg)
			if inErr != nil {
				r.registry.ReportFailure(adapterImpl.ID())
				lastErr = inErr
				continue
			}
			r.registry.ReportSuccess(adapterImpl.ID())
			return adapterImpl, nil, inbound, nil
		}

		outbound, outErr := adapterImpl.OpenOutbound(ctx, cfg)
		if outErr != nil {
			r.registry.ReportFailure(adapterImpl.ID())
			lastErr = outErr
			continue
		}
		r.registry.ReportSuccess(adapterImpl.ID())
		return adapterImpl, outbound, nil, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no adapter could open stream")
	}
	return nil, nil, nil, lastErr
}

func orderProbeCandidates(preferred []string, probes []adapter.ProbeResult) []adapter.ProbeResult {
	if len(preferred) == 0 {
		ordered := make([]adapter.ProbeResult, len(probes))
		copy(ordered, probes)
		return ordered
	}

	ordered := make([]adapter.ProbeResult, 0, len(probes))
	seen := map[string]struct{}{}
	for _, id := range preferred {
		for _, probe := range probes {
			if probe.AdapterID != id {
				continue
			}
			if _, ok := seen[probe.AdapterID]; ok {
				continue
			}
			ordered = append(ordered, probe)
			seen[probe.AdapterID] = struct{}{}
		}
	}
	for _, probe := range probes {
		if _, ok := seen[probe.AdapterID]; ok {
			continue
		}
		ordered = append(ordered, probe)
	}
	return ordered
}

func (r *Runtime) runGC(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = r.store.GC(ctx, time.Minute*10)
		}
	}
}

func generateSessionID(streamID string) string {
	seed := time.Now().UnixNano()
	n := rand.Int63()
	return fmt.Sprintf("%s-%d-%d", strings.ReplaceAll(streamID, " ", "_"), seed, n)
}
