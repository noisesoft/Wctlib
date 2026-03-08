package core

import (
	"context"
	"time"
	"sync"

	"github.com/noisesoft/Wctlib/pkg/wctlib/telemetry"
)

type Scheduler struct {
	workers int
	tasks   chan func(context.Context) error
	wg      sync.WaitGroup
	closeCh chan struct{}
	metrics telemetry.MetricSink
	name    string
}

func NewScheduler(workers int) *Scheduler {
	return NewSchedulerWithMetrics("default", workers, telemetry.NewNoopMetrics())
}

func NewSchedulerWithMetrics(name string, workers int, metrics telemetry.MetricSink) *Scheduler {
	if workers < 1 {
		workers = 1
	}
	return &Scheduler{
		workers: workers,
		tasks:   make(chan func(context.Context) error, workers*1024),
		closeCh: make(chan struct{}),
		metrics: metrics,
		name:    name,
	}
}

func (s *Scheduler) labels() telemetry.Labels {
	return telemetry.Labels{
		"scheduler": s.name,
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			labels := s.labels()
			for {
				select {
				case <-ctx.Done():
					return
				case <-s.closeCh:
					return
				case task, ok := <-s.tasks:
					if !ok {
						return
					}
					start := time.Now()
					s.metrics.Inc("scheduler_task_started_total", 1, labels)
					s.metrics.Observe("scheduler_queue_depth", float64(len(s.tasks)), labels)
					err := task(ctx)
					s.metrics.Observe("scheduler_task_duration_ms", float64(time.Since(start).Milliseconds()), labels)
					s.metrics.Observe("scheduler_queue_depth", float64(len(s.tasks)), labels)
					if err != nil {
						s.metrics.Inc("scheduler_task_error_total", 1, labels)
					}
				}
			}
		}()
	}
}

func (s *Scheduler) Stop() {
	close(s.closeCh)
	s.wg.Wait()
	close(s.tasks)
}

func (s *Scheduler) Submit(task func(context.Context) error) bool {
	select {
	case s.tasks <- task:
		s.metrics.Inc("scheduler_task_submitted_total", 1, s.labels())
		s.metrics.Observe("scheduler_queue_depth", float64(len(s.tasks)), s.labels())
		return true
	default:
		s.metrics.Inc("scheduler_task_rejected_total", 1, s.labels())
		return false
	}
}
