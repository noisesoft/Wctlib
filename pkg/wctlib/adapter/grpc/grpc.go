package grpcadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"errors"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/adapter"
	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	adapterID      = "grpc"
	grpcServiceID  = "wctlib.StreamTransport"
	grpcMethodPath = "/wctlib.StreamTransport/PushFrame"
)

var defaultCodec = grpcJSONCodec{}

type Adapter struct {
	upstream string
}

func New(upstream string) *Adapter {
	return &Adapter{upstream: upstream}
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
	maxBuf := cfg.Profile.MaxOutstanding
	if maxBuf <= 0 {
		maxBuf = 256
	}
	in := &grpcInbound{frames: make(chan types.Frame, maxBuf), errors: make(chan error, 4)}
	listen, err := normalizeGRPCEndpoint(cfg.Request.Metadata["grpc.listen"])
	if err != nil {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "invalid grpc.listen", Err: err}
	}
	server := grpc.NewServer(grpc.ForceServerCodec(defaultCodec))
	in.server = server
	in.address = listen
	server.RegisterService(&grpcServiceDesc, &grpcTransportHandler{inbound: in})
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "grpc listen", Err: err}
	}
	in.listener = listener
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			select {
			case in.errors <- adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "serve error", Err: err}:
			default:
			}
		}
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
	endpoint, err := normalizeGRPCEndpoint(a.upstream)
	if err != nil {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "invalid grpc upstream", Err: err}
	}
	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(defaultCodec)))
	if err != nil {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "grpc dial", Err: err}
	}
	return &grpcOutbound{endpoint: endpoint, conn: conn}, nil
}

type grpcOutbound struct {
	endpoint string
	conn     *grpc.ClientConn
}

func (g *grpcOutbound) Send(_ context.Context, frame *types.Frame) error {
	if g.endpoint == "" {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "grpc upstream is empty", Err: nil}
	}
	if frame == nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "frame is nil", Err: nil}
	}
	payload := append([]byte(nil), frame.Payload...)
	request := &grpcFrame{
		StreamID:  frame.StreamID,
		SessionID: frame.SessionID,
		Seq:       frame.Seq,
		Offset:    frame.Offset,
		Payload:   payload,
		Last:      frame.Last,
		CreatedAt: frame.CreatedAt.UnixNano(),
	}
	reply := new(grpcAck)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := g.conn.Invoke(ctx, grpcMethodPath, request, reply, grpc.ForceCodec(defaultCodec)); err != nil {
		return adapter.Error{
			Adapter: adapterID,
			Code:    grpcStatusCode(err),
			Reason:  "invoke error",
			Err:     err,
		}
	}
	if !reply.Accepted {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "frame rejected", Err: nil}
	}
	return nil
}

func (g *grpcOutbound) Flush(_ context.Context) error { return nil }
func (g *grpcOutbound) Close(_ context.Context) error {
	if g.conn == nil {
		return nil
	}
	return g.conn.Close()
}

type grpcInbound struct {
	frames   chan types.Frame
	errors   chan error
	server   *grpc.Server
	listener net.Listener
	address  string
	once     sync.Once
}

func (i *grpcInbound) Frames(_ context.Context) <-chan types.Frame { return i.frames }
func (i *grpcInbound) Errors() <-chan error                        { return i.errors }
func (i *grpcInbound) Ack(context.Context, uint64, int64) error {
	return nil
}
func (i *grpcInbound) Close(_ context.Context) error {
	var err error
	i.once.Do(func() {
		i.server.Stop()
		if i.listener != nil {
			_ = i.listener.Close()
		}
		close(i.errors)
		close(i.frames)
	})
	return err
}

type grpcTransportHandler struct {
	inbound *grpcInbound
}

type grpcTransportServer interface {
	PushFrame(context.Context, *grpcFrame) (*grpcAck, error)
}

type grpcFrame struct {
	StreamID  string `json:"stream_id"`
	SessionID string `json:"session_id"`
	Seq       uint64 `json:"seq"`
	Offset    int64  `json:"offset"`
	Last      bool   `json:"last"`
	Payload   []byte `json:"payload"`
	CreatedAt int64  `json:"created_at"`
}

type grpcAck struct {
	Accepted bool `json:"accepted"`
}

type grpcJSONCodec struct{}

func (grpcJSONCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (grpcJSONCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (grpcJSONCodec) Name() string { return "json-wctlib" }

func (s *grpcTransportHandler) PushFrame(ctx context.Context, in *grpcFrame) (*grpcAck, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "empty frame")
	}
	if in.StreamID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing stream id")
	}
	frame := types.Frame{
		StreamID:  in.StreamID,
		SessionID: in.SessionID,
		Seq:       in.Seq,
		Offset:    in.Offset,
		Payload:   in.Payload,
		Last:      in.Last,
	}
	if in.CreatedAt > 0 {
		frame.CreatedAt = time.Unix(0, in.CreatedAt)
	}
	select {
	case s.inbound.frames <- frame:
		return &grpcAck{Accepted: true}, nil
	case <-ctx.Done():
		return nil, status.Error(codes.Canceled, "inbound canceled")
	case <-time.After(250 * time.Millisecond):
		return nil, status.Error(codes.Unavailable, "inbound queue full")
	}
}

var grpcServiceDesc = grpc.ServiceDesc{
	ServiceName: grpcServiceID,
	HandlerType: (*grpcTransportServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "PushFrame",
			Handler:    grpcUnaryPushFrameHandler,
		},
	},
	Streams:  nil,
	Metadata: "wctlib grpc transport",
}

func grpcUnaryPushFrameHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(grpcFrame)
	if err := dec(in); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if interceptor == nil {
		return srv.(grpcTransportServer).PushFrame(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: grpcMethodPath,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(grpcTransportServer).PushFrame(ctx, req.(*grpcFrame))
	}
	return interceptor(ctx, in, info, handler)
}

func grpcStatusCode(err error) adapter.ErrorCode {
	if err == nil {
		return adapter.ErrCodeTransport
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded:
			return adapter.ErrCodeConnect
		case codes.InvalidArgument:
			return adapter.ErrCodeUnsupported
		}
	}
	return adapter.ErrCodeTransport
}

func normalizeGRPCEndpoint(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty endpoint")
	}
	if strings.HasPrefix(raw, "unix://") {
		return "", errors.New("unix sockets are not supported")
	}
	if strings.Contains(raw, "://") {
		parsed, err := url.Parse(raw)
		if err != nil {
			return "", err
		}
		if parsed.Scheme != "" && parsed.Scheme != "grpc" && parsed.Scheme != "http" && parsed.Scheme != "https" && parsed.Scheme != "grpcs" {
			return "", fmt.Errorf("unsupported scheme: %s", parsed.Scheme)
		}
		if parsed.Scheme == "grpcs" {
			return "", errors.New("grpcs scheme is currently unsupported")
		}
		raw = parsed.Host
	}
	if raw == "" {
		return "", fmt.Errorf("invalid endpoint")
	}
	return raw, nil
}
