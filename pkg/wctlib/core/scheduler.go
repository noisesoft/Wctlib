package core

import (
	"context"
	"sync"
)

type Scheduler struct {
	workers int
	tasks   chan func(context.Context) error
	wg      sync.WaitGroup
	closeCh chan struct{}
}

func NewScheduler(workers int) *Scheduler {
	if workers < 1 {
		workers = 1
	}
	return &Scheduler{
		workers: workers,
		tasks:   make(chan func(context.Context) error, workers*1024),
		closeCh: make(chan struct{}),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-s.closeCh:
					return
				case task := <-s.tasks:
					_ = task(ctx)
				}
			}
		}()
	}
}

func (s *Scheduler) Stop() {
	close(s.closeCh)
	s.wg.Wait()
}

func (s *Scheduler) Submit(task func(context.Context) error) bool {
	select {
	case s.tasks <- task:
		return true
	default:
		return false
	}
}
