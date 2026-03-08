package httpadapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/adapter"
	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

const adapterID = "http"

var defaultClient = &http.Client{Timeout: 8 * time.Second}

type Adapter struct {
	endpoint string
	client   *http.Client
}

func New(endpoint string) *Adapter {
	return &Adapter{endpoint: endpoint, client: defaultClient}
}

func (a *Adapter) ID() string      { return adapterID }
func (a *Adapter) Version() string { return "1.0" }
func (a *Adapter) Priority() int   { return 80 }
func (a *Adapter) Capabilities() adapter.Capability {
	return adapter.Capability{Receive: true, Send: true, Resume: true, MaxSize: 1024 * 1024}
}

func (a *Adapter) Supports(_ context.Context, cfg adapter.AdapterConfig) bool {
	if cfg.Request.AdapterHint != "" && cfg.Request.AdapterHint != adapterID {
		return false
	}
	if cfg.Profile.ChunkSize > a.Capabilities().MaxSize {
		return false
	}
	if cfg.Direction == types.DirectionReceive {
		return cfg.Request.Metadata != nil && cfg.Request.Metadata["http.listen"] != ""
	}
	if a.endpoint == "" {
		return false
	}
	return true
}

func (a *Adapter) OpenInbound(_ context.Context, cfg adapter.AdapterConfig) (adapter.InboundEndpoint, error) {
	listen := cfg.Request.Metadata["http.listen"]
	if listen == "" {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeUnsupported, Reason: "missing http.listen metadata", Err: nil}
	}
	handler, ok := cfg.Request.Metadata["http.path"]
	if !ok || handler == "" {
		handler = "/wctlib/inbound"
	}
	in := &httpInbound{frames: make(chan types.Frame, cfg.Profile.MaxOutstanding), errors: make(chan error, 4)}
	mux := http.NewServeMux()
	mux.HandleFunc(handler, in.handleRequest)
	srv := &http.Server{Addr: listen, Handler: mux}
	in.server = srv
	go func() {
		_ = srv.ListenAndServe()
		close(in.errors)
	}()
	return in, nil
}

func (a *Adapter) OpenOutbound(_ context.Context, cfg adapter.AdapterConfig) (adapter.OutboundEndpoint, error) {
	if a.endpoint == "" {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeUnsupported, Reason: "missing endpoint", Err: nil}
	}
	if cfg.Profile.ChunkSize > a.Capabilities().MaxSize {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeUnsupported, Reason: "chunk size exceeds max size", Err: nil}
	}
	return &httpOutbound{endpoint: a.endpoint, client: a.client, profile: cfg.Profile}, nil
}

type httpOutbound struct {
	endpoint string
	client   *http.Client
	profile  types.StreamProfile
}

func (o *httpOutbound) Send(ctx context.Context, frame *types.Frame) error {
	buf := bytes.NewBuffer(frame.Payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.endpoint, buf)
	if err != nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "build request", Err: err}
	}
	req.Header.Set("x-wctlib-stream", frame.StreamID)
	req.Header.Set("x-wctlib-session", frame.SessionID)
	req.Header.Set("x-wctlib-seq", strconv.FormatUint(frame.Seq, 10))
	req.Header.Set("x-wctlib-offset", strconv.FormatInt(frame.Offset, 10))
	resp, err := o.client.Do(req)
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

func (o *httpOutbound) Flush(_ context.Context) error { return nil }
func (o *httpOutbound) Close(_ context.Context) error { return nil }

type httpInbound struct {
	frames chan types.Frame
	errors chan error
	server *http.Server
}

func (i *httpInbound) Frames(_ context.Context) <-chan types.Frame { return i.frames }
func (i *httpInbound) Errors() <-chan error                        { return i.errors }
func (i *httpInbound) Ack(context.Context, uint64, int64) error {
	return nil
}
func (i *httpInbound) Close(_ context.Context) error {
	err := i.server.Close()
	return err
}
func (i *httpInbound) handleRequest(w http.ResponseWriter, r *http.Request) {
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
	frame := types.Frame{StreamID: headers.Get("x-wctlib-stream"), SessionID: headers.Get("x-wctlib-session"), Seq: seq, Offset: offset, Payload: payload}
	select {
	case i.frames <- frame:
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusTooManyRequests)
	}
}
