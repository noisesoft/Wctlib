package webrtcadapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/adapter"
	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
	webrtc "github.com/pion/webrtc/v3"
)

const (
	adapterID            = "webrtc"
	adapterVersion       = "1.0"
	adapterPriority      = 70
	adapterMaxFrameSize  = 4 * 1024 * 1024
	defaultInboundBuffer = 256

	defaultSignalPollInterval = 700 * time.Millisecond
	defaultSignalTimeout      = 45 * time.Second
	defaultSignalOfferPath    = "/offer"
	defaultSignalAnswerPath   = "/answer"
	offerSignalRole           = "sender"
	answerSignalRole          = "receiver"
	signalPendingState        = "pending"
	frameMagic                = 0x7f
)

const (
	frameHeaderSize = 1 + 8 + 8 + 1 + 8 + 4 + 4 + 4
)

type signalPayload struct {
	Room      string `json:"room"`
	StreamID  string `json:"stream_id"`
	SessionID string `json:"session_id"`
	Role      string `json:"role"`
	SDP       string `json:"sdp"`
}

type signalResponse struct {
	SDP   string `json:"sdp"`
	State string `json:"state"`
	Err   string `json:"error"`
}

type signalingConfig struct {
	offerURL     string
	answerURL    string
	room         string
	streamID     string
	sessionID    string
	pollInterval time.Duration
	pollTimeout  time.Duration
}

type Adapter struct {
	roomID string
}

func New(roomID string) *Adapter {
	return &Adapter{roomID: roomID}
}

func (a *Adapter) ID() string      { return adapterID }
func (a *Adapter) Version() string { return adapterVersion }
func (a *Adapter) Priority() int   { return adapterPriority }

func (a *Adapter) Capabilities() adapter.Capability {
	return adapter.Capability{Receive: true, Send: true, Resume: false, MaxSize: adapterMaxFrameSize}
}

func (a *Adapter) Supports(_ context.Context, cfg adapter.AdapterConfig) bool {
	if cfg.Request.AdapterHint != "" && cfg.Request.AdapterHint != adapterID {
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
	_, hasSignal := parseSignalingMetadata(cfg.Request.Metadata, cfg.StreamID, cfg.SessionID)
	return hasSignal
}

func (a *Adapter) room(cfg adapter.AdapterConfig) (string, error) {
	room := metadataValue(cfg.Request.Metadata, "webrtc.room")
	if room == "" {
		room = a.roomID
	}
	if room == "" {
		return "", adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "missing webrtc room", Err: nil}
	}
	return room, nil
}

func (a *Adapter) OpenInbound(ctx context.Context, cfg adapter.AdapterConfig) (adapter.InboundEndpoint, error) {
	room, err := a.room(cfg)
	if err != nil {
		return nil, err
	}
	signal, ok := parseSignalingMetadata(cfg.Request.Metadata, cfg.StreamID, cfg.SessionID)
	if !ok {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeUnsupported, Reason: "missing signaling metadata", Err: nil}
	}
	signal.room = room

	peer, err := createPeerConnection(cfg)
	if err != nil {
		return nil, err
	}

	maxBuf := cfg.Profile.MaxOutstanding
	if maxBuf <= 0 {
		maxBuf = defaultInboundBuffer
	}

	in := &webrtcInbound{
		room:    room,
		stream:  cfg.StreamID,
		session: cfg.SessionID,
		frames:  make(chan types.Frame, maxBuf),
		errors:  make(chan error, 4),
		closed:  make(chan struct{}),
		peer:    peer,
	}

	peer.OnDataChannel(func(dc *webrtc.DataChannel) {
		if dc == nil || dc.Label() != cfg.StreamID {
			return
		}
		in.attachDataChannel(dc)
	})

	go in.establish(ctx, signal)

	return in, nil
}

func (a *Adapter) OpenOutbound(ctx context.Context, cfg adapter.AdapterConfig) (adapter.OutboundEndpoint, error) {
	room, err := a.room(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Profile.ChunkSize > a.Capabilities().MaxSize {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeUnsupported, Reason: "chunk size exceeds max size", Err: nil}
	}
	signal, ok := parseSignalingMetadata(cfg.Request.Metadata, cfg.StreamID, cfg.SessionID)
	if !ok {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeUnsupported, Reason: "missing signaling metadata", Err: nil}
	}
	signal.room = room

	peer, err := createPeerConnection(cfg)
	if err != nil {
		return nil, err
	}

	opts := webrtc.DataChannelInit{Ordered: &cfg.Profile.Ordered}
	channel, err := peer.CreateDataChannel(cfg.StreamID, &opts)
	if err != nil {
		_ = peer.Close()
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "create data channel", Err: err}
	}

	out := &webrtcOutbound{
		peer:   peer,
		data:   channel,
		signal: signal,
		ready:  make(chan struct{}),
		closed: make(chan struct{}),
	}

	channel.OnOpen(func() { out.setReady(nil) })
	channel.OnError(func(channelErr error) {
		if channelErr == nil {
			return
		}
		out.setReady(adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "data channel error", Err: channelErr})
	})
	channel.OnClose(func() {
		out.setReady(adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "data channel closed", Err: nil})
	})

	go out.establish(ctx)

	return out, nil
}

type webrtcOutbound struct {
	peer   *webrtc.PeerConnection
	data   *webrtc.DataChannel
	signal signalingConfig

	ready     chan struct{}
	readyErr  error
	readyMu   sync.Mutex
	readyOnce sync.Once

	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func (o *webrtcOutbound) waitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.closed:
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "outbound closed", Err: o.closeErr}
	case <-o.ready:
		o.readyMu.Lock()
		err := o.readyErr
		o.readyMu.Unlock()
		return err
	}
}

func (o *webrtcOutbound) setReady(err error) {
	o.readyMu.Lock()
	if o.readyErr == nil {
		o.readyErr = err
	}
	o.readyMu.Unlock()
	o.readyOnce.Do(func() { close(o.ready) })
}

func (o *webrtcOutbound) Send(ctx context.Context, frame *types.Frame) error {
	if frame == nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "frame is nil", Err: nil}
	}
	if frame.StreamID == "" {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "missing stream id", Err: nil}
	}
	if frame.StreamID != o.signal.streamID {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "stream id mismatch", Err: nil}
	}
	if frame.Offset < 0 {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "offset must be non-negative", Err: nil}
	}
	if len(frame.Payload) > adapterMaxFrameSize {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "payload too large", Err: nil}
	}
	if frame.CreatedAt.IsZero() {
		frame.CreatedAt = time.Now().UTC()
	}
	if err := o.waitReady(ctx); err != nil {
		return err
	}
	encoded, err := encodeFrame(*frame)
	if err != nil {
		return err
	}
	if o.data == nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "data channel is missing", Err: nil}
	}
	if err := o.data.Send(encoded); err != nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "send failed", Err: err}
	}
	return nil
}

func (o *webrtcOutbound) Flush(_ context.Context) error { return o.waitReady(context.Background()) }

func (o *webrtcOutbound) Close(ctx context.Context) error {
	o.closeOnce.Do(func() {
		o.closeErr = adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "closed", Err: ctx.Err()}
		close(o.closed)
		o.setReady(o.closeErr)
		if o.peer != nil {
			_ = o.peer.Close()
		}
	})
	return nil
}

func (o *webrtcOutbound) establish(ctx context.Context) {
	if err := o.connectSender(ctx); err != nil {
		o.setReady(err)
	}
}

func (o *webrtcOutbound) connectSender(ctx context.Context) error {
	offer, err := o.peer.CreateOffer(nil)
	if err != nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "create offer", Err: err}
	}
	if err = o.peer.SetLocalDescription(offer); err != nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "set local offer", Err: err}
	}

	<-webrtc.GatheringCompletePromise(o.peer)

	local := o.peer.LocalDescription()
	if local == nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "empty local description", Err: nil}
	}

	response, err := sendSignal(ctx, o.signal, offerSignalRole, local.SDP)
	if err != nil {
		return err
	}

	sdp := ""
	if response != nil {
		sdp = response.SDP
	}
	if sdp == "" {
		sdp, err = pollSignalForSDP(ctx, o.signal, answerSignalRole)
		if err != nil {
			return err
		}
	}
	if sdp == "" {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "missing answer sdp", Err: nil}
	}
	if err = o.peer.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sdp}); err != nil {
		return adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "set remote answer", Err: err}
	}

	o.setReady(nil)
	return nil
}

type webrtcInbound struct {
	room    string
	stream  string
	session string
	peer    *webrtc.PeerConnection
	frames  chan types.Frame
	errors  chan error
	closed  chan struct{}
	once    sync.Once
}

func (i *webrtcInbound) Frames(_ context.Context) <-chan types.Frame { return i.frames }
func (i *webrtcInbound) Errors() <-chan error                        { return i.errors }
func (i *webrtcInbound) Ack(context.Context, uint64, int64) error    { return nil }

func (i *webrtcInbound) Close(ctx context.Context) error {
	i.once.Do(func() {
		close(i.closed)
		if i.peer != nil {
			_ = i.peer.Close()
		}
		close(i.frames)
		close(i.errors)
	})
	return nil
}

func (i *webrtcInbound) establish(ctx context.Context, signal signalingConfig) {
	offerSDP, err := pollSignalForSDP(ctx, signal, offerSignalRole)
	if err != nil {
		i.reportError(err)
		_ = i.Close(context.Background())
		return
	}
	if offerSDP == "" {
		i.reportError(adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "missing offer sdp", Err: nil})
		_ = i.Close(context.Background())
		return
	}

	if err := i.peer.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offerSDP}); err != nil {
		i.reportError(adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "set remote offer", Err: err})
		_ = i.Close(context.Background())
		return
	}

	answer, err := i.peer.CreateAnswer(nil)
	if err != nil {
		i.reportError(adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "create answer", Err: err})
		_ = i.Close(context.Background())
		return
	}
	if err := i.peer.SetLocalDescription(answer); err != nil {
		i.reportError(adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "set local answer", Err: err})
		_ = i.Close(context.Background())
		return
	}

	if _, err := sendSignal(ctx, signal, answerSignalRole, answer.SDP); err != nil {
		i.reportError(err)
		_ = i.Close(context.Background())
		return
	}
}

func (i *webrtcInbound) attachDataChannel(dc *webrtc.DataChannel) {
	dc.OnMessage(func(message webrtc.DataChannelMessage) {
		if i.isClosed() {
			return
		}
		if message.IsString {
			i.reportError(adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "received string message", Err: nil})
			return
		}
		frame, err := decodeFrame(message.Data)
		if err != nil {
			i.reportError(adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "decode frame", Err: err})
			return
		}
		if frame.StreamID != i.stream {
			i.reportError(adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "stream mismatch", Err: nil})
			return
		}
		if frame.SessionID != "" && i.session != "" && frame.SessionID != i.session {
			i.reportError(adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "session mismatch", Err: nil})
			return
		}
		if frame.SessionID == "" {
			frame.SessionID = i.session
		}
		select {
		case i.frames <- frame:
		case <-i.closed:
		case <-time.After(500 * time.Millisecond):
			i.reportError(adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "frame channel full", Err: nil})
		}
	})
	dc.OnError(func(err error) {
		if err != nil {
			i.reportError(adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "data channel error", Err: err})
		}
	})
	dc.OnClose(func() {
		_ = i.Close(context.Background())
	})
}

func (i *webrtcInbound) reportError(err error) {
	if err == nil {
		return
	}
	if i.isClosed() {
		return
	}
	var value error
	switch typed := err.(type) {
	case adapter.Error:
		value = typed
	default:
		value = adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "inbound", Err: err}
	}
	defer func() {
		_ = recover()
	}()
	select {
	case i.errors <- value:
	default:
	}
}

func (i *webrtcInbound) isClosed() bool {
	select {
	case <-i.closed:
		return true
	default:
		return false
	}
}

func parseSignalingMetadata(metadata map[string]string, streamID, sessionID string) (signalingConfig, bool) {
	offerURL := metadataValue(metadata,
		"webrtc.offer_url", "webrtc.offer-url", "webrtc.signaling.offer", "webrtc.offer",
	)
	answerURL := metadataValue(metadata,
		"webrtc.answer_url", "webrtc.answer-url", "webrtc.signaling.answer", "webrtc.answer",
	)
	baseURL := metadataValue(metadata, "webrtc.signal", "webrtc.signaling", "webrtc.signaling_url", "webrtc.signaling-url")

	if offerURL == "" {
		offerURL = baseURL
	}
	if answerURL == "" {
		answerURL = baseURL
	}
	if offerURL == "" && answerURL == "" {
		return signalingConfig{}, false
	}

	if offerURL == "" {
		offerURL = answerURL
	}
	if answerURL == "" {
		answerURL = offerURL
	}

	offerURL, err := ensureSignalPath(offerURL, defaultSignalOfferPath)
	if err != nil {
		return signalingConfig{}, false
	}
	answerURL, err = ensureSignalPath(answerURL, defaultSignalAnswerPath)
	if err != nil {
		return signalingConfig{}, false
	}

	pollInterval := parseDuration(metadataValue(metadata,
		"webrtc.poll_interval", "webrtc.signal_poll_interval", "webrtc.signaling_poll_interval",
	), defaultSignalPollInterval)
	pollTimeout := parseDuration(metadataValue(metadata,
		"webrtc.poll_timeout", "webrtc.signal_poll_timeout", "webrtc.signaling_poll_timeout",
	), defaultSignalTimeout)

	return signalingConfig{
		offerURL:     offerURL,
		answerURL:    answerURL,
		streamID:     streamID,
		sessionID:    sessionID,
		pollInterval: pollInterval,
		pollTimeout:  pollTimeout,
	}, true
}

func ensureSignalPath(raw string, fallback string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if parsed.Path == "" || parsed.Path == "/" {
		parsed.Path = fallback
	}
	return parsed.String(), nil
}

func createPeerConnection(cfg adapter.AdapterConfig) (*webrtc.PeerConnection, error) {
	peer, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: buildICEServers(cfg)})
	if err != nil {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "create peer connection", Err: err}
	}
	return peer, nil
}

func buildICEServers(cfg adapter.AdapterConfig) []webrtc.ICEServer {
	stun := splitList(metadataValue(cfg.Request.Metadata, "webrtc.stun", "webrtc.stun_servers", "webrtc.stunServers"))
	turn := splitList(metadataValue(cfg.Request.Metadata, "webrtc.turn", "webrtc.turn_servers", "webrtc.turnServers"))

	servers := make([]webrtc.ICEServer, 0, len(stun)+len(turn))
	seen := make(map[string]struct{})
	appendServer := func(u, user, pass string, credType webrtc.ICECredentialType) {
		u = strings.TrimSpace(u)
		if u == "" {
			return
		}
		if _, ok := seen[u]; ok {
			return
		}
		seen[u] = struct{}{}
		server := webrtc.ICEServer{URLs: []string{u}}
		if user != "" {
			server.Username = user
			server.Credential = pass
			server.CredentialType = credType
		}
		servers = append(servers, server)
	}

	for _, item := range stun {
		appendServer(item, "", "", webrtc.ICECredentialTypePassword)
	}
	if len(turn) > 0 {
		user := metadataValue(cfg.Request.Metadata, "webrtc.turn_user", "webrtc.turn.username", "webrtc.turn.user")
		pass := metadataValue(cfg.Request.Metadata, "webrtc.turn_pass", "webrtc.turn.password", "webrtc.turn.pass")
		credName := metadataValue(cfg.Request.Metadata, "webrtc.turn_credential_type", "webrtc.turn.credential_type")
		credType := webrtc.ICECredentialTypePassword
		if strings.EqualFold(credName, "oauth") || strings.EqualFold(credName, "token") {
			credType = webrtc.ICECredentialTypeOauth
		}
		for _, item := range turn {
			appendServer(item, user, pass, credType)
		}
	}
	if len(servers) == 0 {
		appendServer("stun:stun.l.google.com:19302", "", "", webrtc.ICECredentialTypePassword)
	}
	return servers
}

func sendSignal(ctx context.Context, signal signalingConfig, role string, sdp string) (*signalResponse, error) {
	endpoint := signal.answerURL
	if role == offerSignalRole {
		endpoint = signal.offerURL
	}
	reqURL, err := buildSignalRequestURL(endpoint, signal, role)
	if err != nil {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "invalid signaling url", Err: err}
	}

	payload := signalPayload{
		Room:      signal.room,
		StreamID:  signal.streamID,
		SessionID: signal.sessionID,
		Role:      role,
		SDP:       sdp,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "marshal signal payload", Err: err}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(data))
	if err != nil {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "build signal request", Err: err}
	}
	req.Header.Set("content-type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "send signal", Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyData, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: fmt.Sprintf("signal returned %d %s", resp.StatusCode, strings.TrimSpace(string(bodyData))), Err: nil}
	}

	responseData, _ := io.ReadAll(io.LimitReader(resp.Body, 65536))
	if len(responseData) == 0 {
		return &signalResponse{State: signalPendingState}, nil
	}
	var response signalResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "invalid signal response", Err: err}
	}
	if response.Err != "" {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: response.Err, Err: nil}
	}
	return &response, nil
}

func pollSignalForSDP(ctx context.Context, signal signalingConfig, role string) (string, error) {
	interval := signal.pollInterval
	if interval <= 0 {
		interval = defaultSignalPollInterval
	}
	timeout := signal.pollTimeout
	if timeout <= 0 {
		timeout = defaultSignalTimeout
	}

	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return "", adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeConnect, Reason: "signal timeout", Err: context.DeadlineExceeded}
		}
		resp, err := querySignal(ctx, signal, role)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return "", err
			}
		}
		if resp != nil && resp.SDP != "" {
			return resp.SDP, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(interval):
		}
	}
}

func querySignal(ctx context.Context, signal signalingConfig, role string) (*signalResponse, error) {
	endpoint := signal.answerURL
	if role == offerSignalRole {
		endpoint = signal.offerURL
	}
	reqURL, err := buildSignalRequestURL(endpoint, signal, role)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "connection refused") {
			return nil, nil
		}
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusAccepted {
		return &signalResponse{State: signalPendingState}, nil
	}
	if resp.StatusCode >= 300 {
		bodyData, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: fmt.Sprintf("signal poll returned %d %s", resp.StatusCode, strings.TrimSpace(string(bodyData))), Err: nil}
	}
	responseData, _ := io.ReadAll(io.LimitReader(resp.Body, 65536))
	if len(responseData) == 0 {
		return &signalResponse{State: signalPendingState}, nil
	}
	var response signalResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "invalid signal response", Err: err}
	}
	if response.Err != "" {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: response.Err, Err: nil}
	}
	return &response, nil
}

func buildSignalRequestURL(raw string, signal signalingConfig, role string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	q := parsed.Query()
	q.Set("room", signal.room)
	q.Set("stream", signal.streamID)
	q.Set("session", signal.sessionID)
	q.Set("role", role)
	parsed.RawQuery = q.Encode()
	return parsed.String(), nil
}

func parseDuration(raw string, def time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def
	}
	if value, err := time.ParseDuration(raw); err == nil {
		return value
	}
	if secs, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Duration(secs) * time.Second
	}
	return def
}

func metadataValue(metadata map[string]string, keys ...string) string {
	if len(metadata) == 0 {
		return ""
	}
	for _, key := range keys {
		if value := strings.TrimSpace(metadata[key]); value != "" {
			return value
		}
	}
	return ""
}

func splitList(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\t' || r == ' '
	})
	items := make([]string, 0, len(parts))
	for _, value := range parts {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		items = append(items, value)
	}
	return items
}

func encodeFrame(frame types.Frame) ([]byte, error) {
	if frame.StreamID == "" {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "missing stream id", Err: nil}
	}
	if frame.Offset < 0 {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "offset must be non-negative", Err: nil}
	}
	stream := []byte(frame.StreamID)
	session := []byte(frame.SessionID)
	payload := frame.Payload
	if payload == nil {
		payload = []byte{}
	}
	if len(payload) > adapterMaxFrameSize {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "payload exceeds max size", Err: nil}
	}
	if len(stream) > int(^uint32(0)) || len(session) > int(^uint32(0)) || len(payload) > int(^uint32(0)) {
		return nil, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "frame data too large", Err: nil}
	}

	encoded := make([]byte, frameHeaderSize+len(stream)+len(session)+len(payload))
	encoded[0] = frameMagic
	binary.BigEndian.PutUint64(encoded[1:9], frame.Seq)
	binary.BigEndian.PutUint64(encoded[9:17], uint64(frame.Offset))
	if frame.Last {
		encoded[17] = 1
	}
	if frame.CreatedAt.IsZero() {
		frame.CreatedAt = time.Now().UTC()
	}
	binary.BigEndian.PutUint64(encoded[18:26], uint64(frame.CreatedAt.UnixNano()))
	binary.BigEndian.PutUint32(encoded[26:30], uint32(len(stream)))
	binary.BigEndian.PutUint32(encoded[30:34], uint32(len(session)))
	binary.BigEndian.PutUint32(encoded[34:38], uint32(len(payload)))
	copy(encoded[38:38+len(stream)], stream)
	copy(encoded[38+len(stream):38+len(stream)+len(session)], session)
	copy(encoded[38+len(stream)+len(session):], payload)
	return encoded, nil
}

func decodeFrame(raw []byte) (types.Frame, error) {
	if len(raw) < frameHeaderSize {
		return types.Frame{}, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "invalid frame data", Err: nil}
	}
	if raw[0] != frameMagic {
		return types.Frame{}, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "unsupported frame version", Err: nil}
	}
	frame := types.Frame{}
	frame.Seq = binary.BigEndian.Uint64(raw[1:9])
	offset := int64(binary.BigEndian.Uint64(raw[9:17]))
	frame.Offset = offset
	frame.Last = raw[17] == 1
	frame.CreatedAt = time.Unix(0, int64(binary.BigEndian.Uint64(raw[18:26]))).UTC()
	streamLen := int(binary.BigEndian.Uint32(raw[26:30]))
	sessionLen := int(binary.BigEndian.Uint32(raw[30:34]))
	payloadLen := int(binary.BigEndian.Uint32(raw[34:38]))
	if len(raw) < frameHeaderSize+streamLen+sessionLen+payloadLen {
		return types.Frame{}, adapter.Error{Adapter: adapterID, Code: adapter.ErrCodeTransport, Reason: "invalid frame data", Err: nil}
	}
	cursor := 38
	stream := raw[cursor : cursor+streamLen]
	cursor += streamLen
	session := raw[cursor : cursor+sessionLen]
	cursor += sessionLen
	payload := raw[cursor : cursor+payloadLen]

	frame.StreamID = string(stream)
	frame.SessionID = string(session)
	if len(payload) > 0 {
		frame.Payload = make([]byte, len(payload))
		copy(frame.Payload, payload)
	}
	return frame, nil
}
