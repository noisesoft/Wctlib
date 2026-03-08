package wctlib

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/adapter"
	"github.com/noisesoft/Wctlib/pkg/wctlib/core"
	"github.com/noisesoft/Wctlib/pkg/wctlib/store"
	"github.com/noisesoft/Wctlib/pkg/wctlib/telemetry"
	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

var _ adapter.Logger = (*nopLogger)(nil)

type Library struct {
	runtime *core.Runtime
}

type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

type Option func(*libraryConfig) error

type libraryConfig struct {
	registry *adapter.Registry
	store    store.CheckpointStore
	workers  int
	logger   adapter.Logger
	metrics  telemetry.MetricSink
}

func WithWorkers(workers int) Option {
	return func(cfg *libraryConfig) error {
		cfg.workers = workers
		return nil
	}
}

func WithStore(checkpointStore store.CheckpointStore) Option {
	return func(cfg *libraryConfig) error {
		cfg.store = checkpointStore
		return nil
	}
}

func WithLogger(logger adapter.Logger) Option {
	return func(cfg *libraryConfig) error {
		if logger == nil {
			return fmt.Errorf("logger is nil")
		}
		cfg.logger = logger
		return nil
	}
}

func WithMetrics(metrics telemetry.MetricSink) Option {
	return func(cfg *libraryConfig) error {
		if metrics == nil {
			return fmt.Errorf("metrics is nil")
		}
		cfg.metrics = metrics
		return nil
	}
}

func WithRegistry(registry *adapter.Registry) Option {
	return func(cfg *libraryConfig) error {
		if registry == nil {
			return fmt.Errorf("registry is nil")
		}
		cfg.registry = registry
		return nil
	}
}

func New(options ...Option) (*Library, error) {
	cfg := &libraryConfig{
		workers: runtime.NumCPU(),
		logger:  nopLogger{},
		metrics: telemetry.NewNoopMetrics(),
	}
	cfg.registry = adapter.NewRegistry()
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(cfg); err != nil {
			return nil, err
		}
	}

	if cfg.store == nil {
		basePath := filepath.Join(os.TempDir(), "wctlib", "checkpoints")
		durable, err := store.NewFileCheckpointStore(basePath)
		if err != nil {
			cfg.store = store.NewMemoryStore()
		} else {
			cfg.store = durable
		}
	}

	fallbackMemory, err := store.NewDedupStore(store.NewMemoryStore())
	if err != nil {
		return nil, err
	}
	storeWithFallback, err := store.NewFallbackStore(cfg.store, fallbackMemory)
	if err != nil {
		return nil, err
	}
	cfg.store = storeWithFallback

	policy := types.DefaultStreamProfile()
	rt, err := core.NewRuntime(core.RuntimeConfig{
		Registry:       cfg.registry,
		Store:          cfg.store,
		Metrics:        cfg.metrics,
		Logger:         cfg.logger,
		DataWorkers:    cfg.workers,
		ControlWorkers: cfg.workers,
		FlowPolicy: core.FlowPolicy{
			WindowSize:     policy.WindowSize,
			MaxOutstanding: policy.MaxOutstanding,
			ChunkSize:      policy.ChunkSize,
			RetryMax:       policy.RetryMax,
			RetryBaseDelay: policy.RetryBaseDelay,
			RetryMaxDelay:  policy.RetryMaxDelay,
			MaxBytesPerSec: policy.MaxBytesPerSec,
			Adaptive:       policy.Adaptive,
			Ordered:        policy.Ordered,
		},
	})
	if err != nil {
		return nil, err
	}
	return &Library{runtime: rt}, nil
}

func (l *Library) RegisterAdapter(a adapter.TransportAdapter) {
	l.runtime.RegisterAdapter(a)
}

func (l *Library) RegisterAdapterTransport(a adapter.TransportAdapter) {
	l.runtime.RegisterAdapter(a)
}

func (l *Library) StartStream(ctx context.Context, req types.StreamRequest) (*core.StreamSession, error) {
	if req.StreamID == "" {
		req.StreamID = "stream-" + randomID()
	}
	if req.Kind == "" {
		req.Kind = types.StreamKindBinary
	}
	if req.Profile.Name == "" {
		req.Profile = types.DefaultStreamProfile()
	}
	if req.Profile.Direction == 0 {
		req.Profile.Direction = req.Direction
	}
	if req.Direction == 0 {
		req.Direction = req.Profile.Direction
	}
	if req.Direction == 0 {
		req.Direction = types.DirectionSend
	}
	return l.runtime.StartSession(ctx, req)
}

func (l *Library) StopStream(_ context.Context, sessionID string) {
	l.runtime.StopSession(sessionID)
}

func (l *Library) Close(_ context.Context) {
	l.runtime.Stop()
}

func randomID() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

func (l *Library) AdapterRegistry() *adapter.Registry {
	return l.runtime.Registry()
}

func (l *Library) Runtime() *core.Runtime {
	return l.runtime
}
