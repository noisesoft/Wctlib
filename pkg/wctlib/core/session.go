package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/adapter"
	"github.com/noisesoft/Wctlib/pkg/wctlib/buffer"
	"github.com/noisesoft/Wctlib/pkg/wctlib/store"
	"github.com/noisesoft/Wctlib/pkg/wctlib/telemetry"
	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

type StreamSession struct {
	id          string
	streamID    string
	request     types.StreamRequest
	policy      FlowPolicy
	adapterName string

	checkpointMu sync.Mutex
	checkpoint   types.StreamCheckpoint

	outbound adapter.OutboundEndpoint
	inbound  adapter.InboundEndpoint
	store    store.CheckpointStore
	flow     *FlowController
	ioLimit  *ioLimiter
	buffer   *buffer.Pool
	metrics  telemetry.MetricSink
	logger   adapter.Logger

	stateMu sync.RWMutex
	state   types.SessionState

	doneCh chan struct{}
	errCh  chan error
	cancel context.CancelFunc

	statsMu sync.RWMutex
	stats   types.StreamStats

	lastErr atomic.Pointer[error]
}

func newStreamSession(
	id string,
	streamID string,
	req types.StreamRequest,
	checkpoint types.StreamCheckpoint,
	policy FlowPolicy,
	adp adapter.OutboundEndpoint,
	in adapter.InboundEndpoint,
	store store.CheckpointStore,
	flow *FlowController,
	buf *buffer.Pool,
	metrics telemetry.MetricSink,
	logger adapter.Logger,
) *StreamSession {
	return &StreamSession{
		id:         id,
		streamID:   streamID,
		request:    req,
		checkpoint: checkpoint,
		policy:     policy,
		outbound:   adp,
		inbound:    in,
		store:      store,
		flow:       flow,
		ioLimit:    newIOLimiter(policy.MaxBytesPerSec, int64(policy.WindowSize*policy.ChunkSize)),
		buffer:     buf,
		metrics:    metrics,
		logger:     logger,
		state:      types.SessionStatePreparing,
		doneCh:     make(chan struct{}),
		errCh:      make(chan error, 4),
		stats: types.StreamStats{
			StartedAt: time.Now().UTC(),
		},
	}
}

func (s *StreamSession) ID() string       { return s.id }
func (s *StreamSession) StreamID() string { return s.streamID }
func (s *StreamSession) Adapter() string  { return s.adapterName }

func (s *StreamSession) Wait() error {
	<-s.doneCh
	if err := s.lastErr.Load(); err != nil {
		return *err
	}
	return nil
}

func (s *StreamSession) Stats() types.StreamStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	copy := s.stats
	return copy
}

func (s *StreamSession) ResumeToken() (string, error) {
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()
	token := types.ResumeToken{
		SessionID: s.id,
		StreamID:  s.streamID,
		Offset:    s.checkpoint.LastOffset,
		Sequence:  s.checkpoint.LastSeq,
		Profile:   s.request.Profile.Name,
	}
	return token.String(), nil
}

func (s *StreamSession) Close() {
	s.setState(types.SessionStateFinished)
	if s.cancel != nil {
		s.cancel()
	}
	s.closeEndpoints()
	_ = s.store.Delete(context.Background(), s.id)
	s.metrics.Observe("session_close", 1, telemetry.Labels{
		"stream_id":  s.streamID,
		"session_id": s.id,
		"adapter":    s.adapterName,
	})
	select {
	case <-s.doneCh:
	default:
		close(s.doneCh)
	}
}

func (s *StreamSession) closeEndpoints() {
	if s.outbound != nil {
		_ = s.outbound.Close(context.Background())
	}
	if s.inbound != nil {
		_ = s.inbound.Close(context.Background())
	}
}

func (s *StreamSession) setState(state types.SessionState) {
	s.stateMu.Lock()
	s.state = state
	s.stateMu.Unlock()
}

func (s *StreamSession) runSend(ctx context.Context, payload io.Reader) {
	if payload == nil {
		s.fail(errors.New("payload is nil"))
		return
	}

	if s.outbound == nil {
		s.fail(errors.New("outbound endpoint missing"))
		return
	}
	s.setState(types.SessionStateRunning)

	chunkSize := s.policy.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 32 * 1024
	}
	offset := s.checkpoint.LastOffset
	seq := s.checkpoint.LastSeq

	for {
		select {
		case <-ctx.Done():
			s.fail(ctx.Err())
			return
		default:
		}

		buf := s.buffer.Borrow(chunkSize)
		n, readErr := payload.Read(buf)
		if n == 0 {
			s.buffer.Release(buf)
			if readErr == io.EOF {
				s.done()
				return
			}
			if readErr != nil {
				s.fail(readErr)
				return
			}
			time.Sleep(1 * time.Millisecond)
			continue
		}
		if n > 0 {
			wait := s.ioLimit.mustWait(n)
			if wait > 0 {
				s.metrics.Observe("io_wait_ms", float64(wait.Milliseconds()), telemetry.Labels{
					"stream_id":  s.streamID,
					"session_id": s.id,
					"adapter":    s.adapterName,
				})
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					s.buffer.Release(buf)
					s.fail(ctx.Err())
					return
				case <-timer.C:
				}
			}
			if err := s.flow.Acquire(ctx, 1); err != nil {
				s.buffer.Release(buf)
				s.fail(err)
				return
			}

			frame := types.Frame{
				StreamID:  s.streamID,
				SessionID: s.id,
				Seq:       seq + 1,
				Offset:    offset + int64(n),
				Payload:   append([]byte(nil), buf[:n]...),
				CreatedAt: time.Now().UTC(),
			}
			if readErr == io.EOF {
				frame.Last = true
			}
			if err := s.sendWithRetry(ctx, frame); err != nil {
				s.fail(err)
				s.buffer.Release(buf)
				return
			}
			s.buffer.Release(buf)

			offset += int64(n)
			seq++
			s.updateCheckpoint(offset, seq, len(frame.Payload))
			s.metrics.Observe("frame_payload_bytes", float64(len(frame.Payload)), telemetry.Labels{
				"stream_id":  s.streamID,
				"session_id": s.id,
				"adapter":    s.adapterName,
			})
		}

		if readErr == io.EOF {
			s.done()
			return
		}
		if readErr != nil {
			s.fail(readErr)
			return
		}
	}
}

func (s *StreamSession) sendWithRetry(ctx context.Context, frame types.Frame) error {
	retry := 0
	for {
		start := time.Now()
		err := s.outbound.Send(ctx, &frame)
		if err == nil {
			latency := time.Since(start)
			loss := 0.0
			if retry > 0 {
				loss = 1.0
			}
			s.flow.ObserveFeedback(float64(latency.Milliseconds()), loss)
			s.metrics.Observe("frame_send_ms", float64(latency.Microseconds()), telemetry.Labels{
				"stream_id":  s.streamID,
				"session_id": s.id,
				"adapter":    s.adapterName,
			})
			s.flow.Release(1)
			return nil
		}

		retry++
		if retry > s.policy.RetryMax {
			s.flow.Release(1)
			return fmt.Errorf("send failed after retries: %w", err)
		}

		delay := s.policy.RetryBaseDelay
		if delay <= 0 {
			delay = 20 * time.Millisecond
		}
		for i := 0; i < retry; i++ {
			delay *= 2
		}
		maxDelay := s.policy.RetryMaxDelay
		if maxDelay <= 0 {
			maxDelay = 2 * time.Second
		}
		if delay > maxDelay {
			delay = maxDelay
		}
		jitterBase := int64(delay / 4)
		if jitterBase <= 0 {
			jitterBase = 1
		}
		timer := time.NewTimer(delay + time.Duration(rand.Int63n(jitterBase)))
		s.metrics.Inc("stream_send_retry_total", 1, telemetry.Labels{
			"stream_id":  s.streamID,
			"session_id": s.id,
			"adapter":    s.adapterName,
			"retry":      fmt.Sprintf("%d", retry),
		})
		s.updateRetryStats()
		select {
		case <-ctx.Done():
			timer.Stop()
			s.flow.Release(1)
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *StreamSession) done() {
	s.setState(types.SessionStateFinished)
	s.closeEndpoints()
	_ = s.store.Delete(context.Background(), s.id)
	s.metrics.Timing("stream_duration_ms", time.Now(), telemetry.Labels{
		"stream_id":  s.streamID,
		"session_id": s.id,
		"adapter":    s.adapterName,
	})
	select {
	case <-s.doneCh:
	default:
		close(s.doneCh)
	}
}

func (s *StreamSession) fail(err error) {
	if err == nil {
		return
	}
	s.setState(types.SessionStateFailed)
	s.lastErr.Store(&err)
	s.updateState()
	if s.logger != nil {
		s.logger.Error("stream failed", "session", s.id, "reason", err)
	}
	s.closeEndpoints()
	_ = s.store.Delete(context.Background(), s.id)
	select {
	case s.errCh <- err:
	default:
	}
	select {
	case <-s.doneCh:
	default:
		close(s.doneCh)
	}
}

func (s *StreamSession) updateCheckpoint(offset int64, seq uint64, sentBytes int) {
	s.checkpointMu.Lock()
	s.checkpoint.LastOffset = offset
	s.checkpoint.LastSeq = seq
	s.checkpoint.UpdatedAt = time.Now().UTC()
	s.checkpoint.SessionID = s.id
	s.checkpoint.StreamID = s.streamID
	cp := s.checkpoint
	s.checkpointMu.Unlock()

	s.statsMu.Lock()
	s.stats.SentBytes += int64(sentBytes)
	s.stats.FramesSent++
	s.stats.AcknowledgedBytes = offset
	s.stats.UpdatedAt = time.Now().UTC()
	s.statsMu.Unlock()

	if err := s.store.Save(context.Background(), s.id, cp); err != nil {
		if s.logger != nil {
			s.logger.Warn("checkpoint save failed", "session", s.id, "reason", err)
		}
	}
}

func (s *StreamSession) updateRetryStats() {
	s.statsMu.Lock()
	s.stats.Retries++
	s.stats.UpdatedAt = time.Now().UTC()
	s.statsMu.Unlock()
	s.metrics.Inc("stream_retry_total", 1, telemetry.Labels{
		"stream_id":  s.streamID,
		"session_id": s.id,
		"adapter":    s.adapterName,
	})
}

func (s *StreamSession) updateState() {
	s.stateMu.RLock()
	state := s.state
	s.stateMu.RUnlock()
	if state == types.SessionStateFailed {
		s.metrics.LogEvent("stream_state_failed", telemetry.Labels{"stream_id": s.streamID, "session_id": s.id, "adapter": s.adapterName})
	}
}
