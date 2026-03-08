package types

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"time"
)

const APIVersion = "wctlib/v1"

type StreamKind string

const (
	StreamKindFile   StreamKind = "file"
	StreamKindMedia  StreamKind = "media"
	StreamKindBinary StreamKind = "binary"
)

type StreamDirection int

const (
	DirectionSend StreamDirection = iota
	DirectionReceive
)

type StreamProfile struct {
	Name           string
	Direction      StreamDirection
	WindowSize     int
	MaxOutstanding int
	ChunkSize      int
	RetryMax       int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	MaxBytesPerSec int64
	Ordered        bool
	Resume         bool
	Adaptive       bool
	Preferred      []string
}

func DefaultStreamProfile() StreamProfile {
	return StreamProfile{
		Name:           "default",
		Direction:      DirectionSend,
		WindowSize:     64,
		MaxOutstanding: 256,
		ChunkSize:      64 * 1024,
		RetryMax:       6,
		RetryBaseDelay: 25 * time.Millisecond,
		RetryMaxDelay:  2 * time.Second,
		MaxBytesPerSec: 0,
		Ordered:        true,
		Resume:         true,
		Adaptive:       true,
		Preferred:      []string{"http", "grpc", "webrtc"},
	}
}

type StreamRequest struct {
	StreamID    string
	Kind        StreamKind
	Direction   StreamDirection
	Payload     io.Reader
	Sink        io.Writer
	SizeHint    int64
	Profile     StreamProfile
	Metadata    map[string]string
	AdapterHint string
	ResumeToken string
}

type Frame struct {
	StreamID  string
	SessionID string
	Seq       uint64
	Offset    int64
	Payload   []byte
	Last      bool
	CreatedAt time.Time
}

type StreamCheckpoint struct {
	SessionID       string
	StreamID        string
	LastOffset      int64
	LastSeq         uint64
	LastAckSequence uint64
	PayloadDigest   string
	ProfileDigest   string
	UpdatedAt       time.Time
}

type ResumeToken struct {
	SessionID string
	StreamID  string
	Offset    int64
	Sequence  uint64
	Profile   string
}

func (r ResumeToken) String() string {
	raw, err := json.Marshal(r)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func ParseResumeToken(raw string) (ResumeToken, error) {
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return ResumeToken{}, err
	}
	var token ResumeToken
	if err := json.Unmarshal(payload, &token); err != nil {
		return ResumeToken{}, err
	}
	if token.SessionID == "" || token.StreamID == "" {
		return ResumeToken{}, errors.New("invalid resume token")
	}
	return token, nil
}

type StreamStats struct {
	SentBytes         int64
	ReceivedBytes     int64
	FramesSent        int64
	FramesReceived    int64
	Retries           int64
	NackCount         int64
	AcknowledgedBytes int64
	StartedAt         time.Time
	UpdatedAt         time.Time
}

type SessionState uint8

const (
	SessionStateUnknown SessionState = iota
	SessionStatePreparing
	SessionStateReady
	SessionStateRunning
	SessionStateReconnecting
	SessionStateFinished
	SessionStateFailed
)

type StreamFrameSink interface {
	Consume(ctx context.Context, frame Frame) error
}
