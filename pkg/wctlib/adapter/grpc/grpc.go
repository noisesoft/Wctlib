package grpcadapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/adapter"
	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

const (
	adapterID   = "grpc"
	defaultPath = "/wctlib/grpc"
)

var defaultClient = &http.Client{Timeout: 8 * time.Second}

type Adapter struct {
	upstream string
	client   *http.Client
}

func New(upstream string) *Adapter {
	return &Adapter{upstream: upstream, client: defaultClient}
}

func (a *Adapter) ID() string      { return adapterID }
func (a *Adapter) Version() string { return "1.0" }
func (a *Adapter) Priority() int   { return 90 }
func (a *Adapter) Capabilities() adapter.Capability {
	return adapter.Capability{Receive: true, Send: true, Resume: true, MaxSize: 4 * 1024 * 1024}
}

func (a *Adapter) Supports(_ context.Context, cfg adapter.AdapterConfig) bool {
	if cfg.Request.AdapterHint != "" && cfg.Request.AdapterHint != adapterID {
		return false
	}
	if cfg.Direction == types.DirectionReceive {
		return cfg.Request.Metadata["grpc.listen"] != ""
	}
	if a.upstream == "" {
		return false
	}
	if cfg.Profile.ChunkSize > a.Capabilities().MaxSize {
		return false
	}
	return true
}

func (a *Adapter) OpenInbound(_ context.Context, cfg adapter.AdapterConfig) (adapter.InboundEndpoint, error) {
	if cfg.Request.Metadata["grpc.listen"] == "" {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeUnsupported, Reason: "missing grpc.listen metadata", Err: nil}
	}
	if a.client == nil {
		a.client = defaultClient
	}
	path := cfg.Request.Metadata["grpc.path"]
	if path == "" {
		path = defaultPath
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	maxBuf := cfg.Profile.MaxOutstanding
	if maxBuf <= 0 {
		maxBuf = 256
	}
	in := &grpcInbound{frames: make(chan types.Frame, maxBuf), errors: make(chan error, 4)}
	mux := http.NewServeMux()
	mux.HandleFunc(path, in.handleRequest)
	srv := &http.Server{Addr: cfg.Request.Metadata["grpc.listen"], Handler: mux}
	in.server = srv
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			select {
			case in.errors <- adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "listen error", Err: err}:
			default:
			}
		}
		close(in.errors)
	}()
	return in, nil
}

func (a *Adapter) OpenOutbound(_ context.Context, cfg adapter.AdapterConfig) (adapter.OutboundEndpoint, error) {
	if a.upstream == "" {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "grpc upstream is empty", Err: nil}
	}
	if cfg.Profile.ChunkSize > a.Capabilities().MaxSize {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeUnsupported, Reason: fmt.Sprintf("chunk size exceeds max size %d", a.Capabilities().MaxSize), Err: nil}
	}
	path := cfg.Request.Metadata["grpc.path"]
	if path == "" {
		path = defaultPath
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return &grpcOutbound{endpoint: a.upstream, path: path, client: a.client}, nil
}

type grpcOutbound struct {
	endpoint string
	path     string
	client   *http.Client
}

func (g *grpcOutbound) Send(_ context.Context, frame *types.Frame) error {
	if g.endpoint == "" {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "grpc upstream is empty", Err: nil}
	}
	if g.client == nil {
		g.client = defaultClient
	}
	if frame == nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "frame is nil", Err: nil}
	}
	reqURL := strings.TrimRight(g.endpoint, "/") + g.path
	req, err := http.NewRequest(http.MethodPost, reqURL, bytes.NewBuffer(frame.Payload))
	if err != nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "build request", Err: err}
	}
	req.Header.Set("x-wctlib-stream", frame.StreamID)
	req.Header.Set("x-wctlib-session", frame.SessionID)
	req.Header.Set("x-wctlib-seq", strconv.FormatUint(frame.Seq, 10))
	req.Header.Set("x-wctlib-offset", strconv.FormatInt(frame.Offset, 10))
	if frame.Last {
		req.Header.Set("x-wctlib-last", "1")
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "post frame", Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: fmt.Sprintf("status %d %s", resp.StatusCode, string(body)), Err: nil}
	}
	return nil
}

func (g *grpcOutbound) Flush(_ context.Context) error { return nil }
func (g *grpcOutbound) Close(_ context.Context) error { return nil }

type grpcInbound struct {
	frames chan types.Frame
	errors chan error
	server *http.Server
}

func (i *grpcInbound) Frames(_ context.Context) <-chan types.Frame { return i.frames }
func (i *grpcInbound) Errors() <-chan error                        { return i.errors }
func (i *grpcInbound) Ack(context.Context, uint64, int64) error {
	return nil
}
func (i *grpcInbound) Close(_ context.Context) error {
	err := i.server.Close()
	close(i.errors)
	return err
}
func (i *grpcInbound) handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	headers := r.Header
	seq, _ := strconv.ParseUint(headers.Get("x-wctlib-seq"), 10, 64)
	offset, _ := strconv.ParseInt(headers.Get("x-wctlib-offset"), 10, 64)
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		select {
		case i.errors <- adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "read body", Err: err}:
		default:
		}
		return
	}
	frame := types.Frame{
		StreamID:  headers.Get("x-wctlib-stream"),
		SessionID: headers.Get("x-wctlib-session"),
		Seq:       seq,
		Offset:    offset,
		Payload:   payload,
	}
	if headers.Get("x-wctlib-last") == "1" {
		frame.Last = true
	}
	select {
	case i.frames <- frame:
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusTooManyRequests)
	}
}
