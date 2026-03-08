package wctlib

import (
	"github.com/noisesoft/Wctlib/pkg/wctlib/adapter"
	"github.com/noisesoft/Wctlib/pkg/wctlib/core"
	"github.com/noisesoft/Wctlib/pkg/wctlib/store"
	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

type StreamRequest = types.StreamRequest
type StreamProfile = types.StreamProfile
type StreamKind = types.StreamKind
type StreamDirection = types.StreamDirection
type StreamCheckpoint = types.StreamCheckpoint
type StreamStats = types.StreamStats
type SessionState = types.SessionState
type StreamFrameSink = types.StreamFrameSink

type StreamSession = core.StreamSession

type CheckpointStore = store.CheckpointStore
type DedupStore = store.DedupStore
type FallbackStore = store.FallbackStore

type TransportAdapter = adapter.TransportAdapter

type ErrorCode = adapter.ErrorCode

type AdapterError = adapter.Error

type AdapterConfig = adapter.AdapterConfig

type InboundEndpoint = adapter.InboundEndpoint

type OutboundEndpoint = adapter.OutboundEndpoint

const (
	DirectionSend      = types.DirectionSend
	DirectionReceive   = types.DirectionReceive
	StreamKindFile     = types.StreamKindFile
	StreamKindMedia    = types.StreamKindMedia
	StreamKindBinary   = types.StreamKindBinary
	SessionStateReady  = types.SessionStateReady
	SessionStateFailed = types.SessionStateFailed
)

const (
	ErrCodeProbe       = adapter.ErrCodeProbe
	ErrCodeConnect     = adapter.ErrCodeConnect
	ErrCodeTransport   = adapter.ErrCodeTransport
	ErrCodeUnsupported = adapter.ErrCodeUnsupported
)

func NewDedupStore(inner store.CheckpointStore) (*DedupStore, error) {
	return store.NewDedupStore(inner)
}

func NewFallbackStore(primary, fallback store.CheckpointStore) (*FallbackStore, error) {
	return store.NewFallbackStore(primary, fallback)
}

func ReconcileCheckpoints(existing, incoming types.StreamCheckpoint) types.StreamCheckpoint {
	return store.ReconcileCheckpoints(existing, incoming)
}

func CheckpointEqual(left, right types.StreamCheckpoint) bool {
	return store.CheckpointEqual(left, right)
}
