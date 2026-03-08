package core

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/telemetry"
	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

func (s *StreamSession) runReceive(ctx context.Context) {
	if s.inbound == nil {
		s.fail(errors.New("inbound endpoint missing"))
		return
	}
	frames := s.inbound.Frames(ctx)
	errs := s.inbound.Errors()
	s.setState(types.SessionStateRunning)
	for {
		select {
		case <-ctx.Done():
			s.fail(ctx.Err())
			return
		case recvErr, ok := <-errs:
			if !ok {
				s.done()
				return
			}
			if recvErr != nil {
				s.fail(recvErr)
				return
			}
		case frame, ok := <-frames:
			if !ok {
				s.done()
				return
			}
			if frame.Offset < s.checkpoint.LastOffset {
				continue
			}
			s.handleReceiveFrame(ctx, frame)
		}
	}
}

func (s *StreamSession) handleReceiveFrame(ctx context.Context, frame types.Frame) {
	if s.request.Sink != nil {
		if frame.Payload == nil {
			s.fail(errors.New("receive payload is nil"))
			return
		}
		wait := s.ioLimit.mustWait(len(frame.Payload))
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
				s.fail(ctx.Err())
				return
			case <-timer.C:
			}
		}
		if _, err := s.request.Sink.Write(frame.Payload); err != nil {
			s.fail(err)
			return
		}
	}
	s.statsMu.Lock()
	s.stats.FramesReceived++
	s.stats.ReceivedBytes += int64(len(frame.Payload))
	s.stats.AcknowledgedBytes = frame.Offset
	s.stats.UpdatedAt = time.Now().UTC()
	s.statsMu.Unlock()
	s.metrics.Observe("frame_receive_bytes", float64(len(frame.Payload)), telemetry.Labels{
		"stream_id":  s.streamID,
		"session_id": s.id,
		"adapter":    s.adapterName,
	})
	if s.inbound != nil {
		_ = s.inbound.Ack(ctx, frame.Seq, frame.Offset)
	}
	s.updateCheckpoint(frame.Offset, frame.Seq, len(frame.Payload))
	if frame.Last {
		s.done()
	}
}

func (s *StreamSession) runReceiveWithWriter(_ context.Context, sink io.Writer) {
	_ = sink
}
