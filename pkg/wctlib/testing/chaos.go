package wcttesting

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"
)

type ChaosConfig struct {
	ErrorRate   float64
	Delay       time.Duration
	Jitter      time.Duration
	CorruptRate float64
}

var (
	errChaosRead  = errors.New("chaos read error")
	errChaosWrite = errors.New("chaos write error")
)

type ChaosReader struct {
	source io.Reader
	cfg    ChaosConfig
	rnd    *rand.Rand
	mu     sync.Mutex
}

func NewChaosReader(source io.Reader, cfg ChaosConfig) *ChaosReader {
	return &ChaosReader{
		source: source,
		cfg:    cfg,
		rnd:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (r *ChaosReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	delay := r.cfg.Delay + durationWithJitter(r.cfg.Jitter, r.rnd)
	shouldFail := r.rnd.Float64() < r.cfg.ErrorRate
	shouldCorrupt := r.rnd.Float64() < r.cfg.CorruptRate
	corruptByte := byte(r.rnd.Intn(255))
	r.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	if shouldFail {
		return 0, fmt.Errorf("%w: synthetic", errChaosRead)
	}

	n, err := r.source.Read(p)
	if n > 0 && shouldCorrupt {
		p[0] = corruptByte
	}
	return n, err
}

func (r *ChaosReader) Close() error { return nil }

type ChaosWriter struct {
	target io.Writer
	cfg    ChaosConfig
	rnd    *rand.Rand
	mu     sync.Mutex
}

func NewChaosWriter(target io.Writer, cfg ChaosConfig) *ChaosWriter {
	return &ChaosWriter{
		target: target,
		cfg:    cfg,
		rnd:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (w *ChaosWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	delay := w.cfg.Delay + durationWithJitter(w.cfg.Jitter, w.rnd)
	shouldFail := w.rnd.Float64() < w.cfg.ErrorRate
	w.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	if shouldFail {
		return 0, fmt.Errorf("%w: synthetic", errChaosWrite)
	}
	return w.target.Write(p)
}

func (w *ChaosWriter) Close() error { return nil }

func durationWithJitter(jitter time.Duration, rnd *rand.Rand) time.Duration {
	if jitter <= 0 {
		return 0
	}
	return time.Duration(rnd.Int63n(int64(jitter)))
}
