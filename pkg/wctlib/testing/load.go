package wcttesting

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib"
	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

type LoadPlan struct {
	TotalStreams int
	Workers      int
	Profile      types.StreamProfile
	Request      func(index int) (types.StreamRequest, error)
}

type LoadResult struct {
	TotalStreams       int
	SuccessfulStreams  int
	FailedStreams      int
	RequestedBytes     int64
	TransferredBytes   int64
	Elapsed            time.Duration
	ThroughputBps      float64
	ErrorRate          float64
	LatencyP50        time.Duration
	LatencyP90        time.Duration
	LatencyP99        time.Duration
	LatencyMax        time.Duration
	DurationP99       time.Duration
}

func (r *LoadResult) addLatency(d time.Duration) {
	r.Elapsed += d
}

type streamOutcome struct {
	index       int
	bytes       int64
	requested   int64
	err         error
	start       time.Time
	end         time.Time
	streamStats types.StreamStats
}

func RunLoad(ctx context.Context, lib *wctlib.Library, plan LoadPlan) (*LoadResult, error) {
	if lib == nil {
		return nil, fmt.Errorf("library is nil")
	}
	if plan.TotalStreams <= 0 {
		return nil, fmt.Errorf("total streams must be positive")
	}
	workers := plan.Workers
	if workers <= 0 {
		workers = 1
	}
	if workers > plan.TotalStreams {
		workers = plan.TotalStreams
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int, plan.TotalStreams)
	outcomes := make(chan streamOutcome, plan.TotalStreams)
	var started int32
	var wg sync.WaitGroup

	for i := 0; i < plan.TotalStreams; i++ {
		jobs <- i
	}
	close(jobs)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				outcome := executeStream(ctx, idx, lib, &plan)
				if atomic.AddInt32(&started, 1) == int32(plan.TotalStreams) {
					cancel()
				}
				outcomes <- outcome
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outcomes)
	}()

	result := &LoadResult{
		TotalStreams: plan.TotalStreams,
	}
	var lock sync.Mutex
	var latencies []time.Duration
	var bytes int64
	var requested int64
	var failed int
	var success int

	for outcome := range outcomes {
		lock.Lock()
		result.Elapsed += outcome.end.Sub(outcome.start)
		latencies = append(latencies, outcome.end.Sub(outcome.start))
		requested += outcome.requested
		bytes += outcome.bytes
		if outcome.err != nil {
			failed++
		} else {
			success++
		}
		lock.Unlock()
		if plan.Request != nil {
			_, _ = outcome.index, outcome.streamStats
		}
	}

	result.SuccessfulStreams = success
	result.FailedStreams = failed
	result.RequestedBytes = requested
	result.TransferredBytes = bytes
	if plan.TotalStreams > 0 {
		result.ErrorRate = float64(failed) / float64(plan.TotalStreams)
	}
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		result.LatencyP50 = percentile(latencies, 0.50)
		result.LatencyP90 = percentile(latencies, 0.90)
		result.LatencyP99 = percentile(latencies, 0.99)
		result.LatencyMax = latencies[len(latencies)-1]
	}
	if len(latencies) > 0 && result.TransferredBytes > 0 {
		total := 0 * time.Duration(0)
		for _, duration := range latencies {
			total += duration
		}
		avg := total / time.Duration(len(latencies))
		result.DurationP99 = avg
		result.ThroughputBps = float64(result.TransferredBytes) / avg.Seconds()
	}
	return result, nil
}

func executeStream(ctx context.Context, idx int, lib *wctlib.Library, plan *LoadPlan) streamOutcome {
	var req types.StreamRequest
	if plan.Request != nil {
		request, err := plan.Request(idx)
		if err != nil {
			return streamOutcome{
				index: idx,
				err:   err,
				start: time.Now(),
			}
		}
		req = request
	} else {
		return streamOutcome{
			index: idx,
			err:   fmt.Errorf("request factory is required"),
			start: time.Now(),
		}
	}
	if req.StreamID == "" {
		req.StreamID = fmt.Sprintf("load-%d-%d", time.Now().UnixNano(), rand.Int63())
	}
	if req.Profile.Name == "" {
		req.Profile = plan.Profile
	}
	start := time.Now()
	session, err := lib.StartStream(ctx, req)
	if err != nil {
		return streamOutcome{index: idx, requested: req.SizeHint, err: err, start: start, end: time.Now()}
	}
	statsBefore := session.Stats()
	err = session.Wait()
	statsAfter := session.Stats()
	end := time.Now()
	result := streamOutcome{
		index: idx,
		err:   err,
		start: start,
		end:   end,
		streamStats: statsAfter,
	}
	if req.SizeHint > 0 {
		result.requested = req.SizeHint
	} else {
		result.requested = maxInt64(statsAfter.ReceivedBytes, statsAfter.SentBytes)
	}
	result.bytes = maxInt64(statsAfter.ReceivedBytes, statsAfter.SentBytes)
	if statsAfter.ReceivedBytes != 0 || statsAfter.SentBytes != 0 || statsAfter.AcknowledgedBytes > 0 {
		_ = statsBefore
	}
	return result
}

func NewPayload(size int64) io.Reader {
	return io.LimitReader(randomSource{}, size)
}

type randomSource struct{}

func (randomSource) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = byte(rand.Intn(256))
	}
	return len(p), nil
}

func percentile(values []time.Duration, q float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	target := int(float64(len(values)-1) * q)
	if target < 0 {
		target = 0
	}
	return values[target]
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

