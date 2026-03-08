package webrtcadapter

import (
	"context"
	"fmt"
	"sync"

	"github.com/noisesoft/Wctlib/pkg/wctlib/adapter"
	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

const adapterID = "webrtc"

const (
	roomDefaultCapacity = 128
	busKeyTemplate      = "room:%s:stream:%s"
)

type roomBus struct {
	mu    sync.RWMutex
	topic map[string][]chan types.Frame
}

type busKey struct {
	room   string
	stream string
}

func (b *roomBus) subscribe(k busKey, ch chan types.Frame) func() {
	key := fmt.Sprintf(busKeyTemplate, k.room, k.stream)
	b.mu.Lock()
	b.topic[key] = append(b.topic[key], ch)
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		topics := b.topic[key]
		for i, candidate := range topics {
			if candidate != ch {
				continue
			}
			b.topic[key] = append(topics[:i], topics[i+1:]...)
			break
		}
		if len(b.topic[key]) == 0 {
			delete(b.topic, key)
		}
		b.mu.Unlock()
	}
}

func sendToFrameChan(ctx context.Context, target chan types.Frame, frame types.Frame) (err error) {
	defer func() {
		if recover() != nil {
			err = adapter.Error{
				Adapter: adapterID,
				Code:    adapter.ErrCodeTransport,
				Reason:  "receiver closed",
				Err:     nil,
			}
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case target <- frame:
		return nil
	}
}

func (b *roomBus) publish(ctx context.Context, k busKey, frame types.Frame) error {
	key := fmt.Sprintf(busKeyTemplate, k.room, k.stream)
	b.mu.RLock()
	subscribers := append([]chan types.Frame(nil), b.topic[key]...)
	b.mu.RUnlock()
	if len(subscribers) == 0 {
		return adapter.Error{
			Adapter: adapterID,
			Code:    adapter.ErrCodeTransport,
			Reason:  "no active receivers",
			Err:     nil,
		}
	}
	for _, ch := range subscribers {
		if err := sendToFrameChan(ctx, ch, frame); err != nil {
			return err
		}
	}
	return nil
}

var memoryBus = &roomBus{topic: map[string][]chan types.Frame{}}

type Adapter struct {
	roomID string
}

func New(roomID string) *Adapter {
	return &Adapter{roomID: roomID}
}

func (a *Adapter) ID() string      { return adapterID }
func (a *Adapter) Version() string { return "1.0" }
func (a *Adapter) Priority() int   { return 70 }
func (a *Adapter) Capabilities() adapter.Capability {
	return adapter.Capability{Receive: true, Send: true, Resume: false, MaxSize: 512 * 1024}
}

func (a *Adapter) Supports(_ context.Context, cfg adapter.AdapterConfig) bool {
	if cfg.Request.AdapterHint != "" && cfg.Request.AdapterHint != adapterID {
		return false
	}
	if a.roomID == "" && cfg.Request.Metadata["webrtc.room"] == "" {
		return false
	}
	if cfg.StreamID == "" {
		return false
	}
	if cfg.Profile.Resume {
		return false
	}
	if cfg.Profile.ChunkSize > a.Capabilities().MaxSize {
		return false
	}
	return true
}

func (a *Adapter) room(cfg adapter.AdapterConfig) string {
	if cfg.Request.Metadata["webrtc.room"] != "" {
		return cfg.Request.Metadata["webrtc.room"]
	}
	return a.roomID
}

func (a *Adapter) OpenInbound(_ context.Context, cfg adapter.AdapterConfig) (adapter.InboundEndpoint, error) {
	room := a.room(cfg)
	if room == "" {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "webrtc room is empty", Err: nil}
	}
	maxBuf := cfg.Profile.MaxOutstanding
	if maxBuf <= 0 {
		maxBuf = roomDefaultCapacity
	}
	in := &webrtcInbound{
		frames: make(chan types.Frame, maxBuf),
		errors: make(chan error, 4),
	}
	in.unsubscribe = memoryBus.subscribe(busKey{room: room, stream: cfg.StreamID}, in.frames)
	return in, nil
}

func (a *Adapter) OpenOutbound(_ context.Context, cfg adapter.AdapterConfig) (adapter.OutboundEndpoint, error) {
	room := a.room(cfg)
	if room == "" {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "webrtc room is empty", Err: nil}
	}
	if cfg.Profile.ChunkSize > a.Capabilities().MaxSize {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeUnsupported, Reason: fmt.Sprintf("chunk size exceeds max size %d", a.Capabilities().MaxSize), Err: nil}
	}
	return &webrtcOutbound{room: room}, nil
}

type webrtcOutbound struct {
	room string
}

func (w *webrtcOutbound) Send(ctx context.Context, frame *types.Frame) error {
	if w.room == "" {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "missing room id", Err: nil}
	}
	if frame == nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "frame is nil", Err: nil}
	}
	if frame.StreamID == "" {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "missing stream id", Err: nil}
	}
	if err := memoryBus.publish(ctx, busKey{room: w.room, stream: frame.StreamID}, *frame); err != nil {
		return err
	}
	return nil
}

func (w *webrtcOutbound) Flush(context.Context) error { return nil }
func (w *webrtcOutbound) Close(context.Context) error { return nil }

type webrtcInbound struct {
	frames      chan types.Frame
	errors      chan error
	unsubscribe func()
}

func (i *webrtcInbound) Frames(_ context.Context) <-chan types.Frame { return i.frames }
func (i *webrtcInbound) Errors() <-chan error                        { return i.errors }
func (i *webrtcInbound) Ack(context.Context, uint64, int64) error {
	return nil
}
func (i *webrtcInbound) Close(_ context.Context) error {
	if i.unsubscribe != nil {
		i.unsubscribe()
		i.unsubscribe = nil
	}
	close(i.errors)
	close(i.frames)
	return nil
}
