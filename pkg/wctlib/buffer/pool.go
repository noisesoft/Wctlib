package buffer

import "sync"

const (
	SmallSize  = 8 * 1024
	MediumSize = 32 * 1024
	LargeSize  = 128 * 1024
)

type Pool struct {
	small  sync.Pool
	medium sync.Pool
	large  sync.Pool
}

func NewPool() *Pool {
	return &Pool{
		small:  sync.Pool{New: func() any { b := make([]byte, SmallSize); return &b }},
		medium: sync.Pool{New: func() any { b := make([]byte, MediumSize); return &b }},
		large:  sync.Pool{New: func() any { b := make([]byte, LargeSize); return &b }},
	}
}

func (p *Pool) Borrow(size int) []byte {
	if size <= SmallSize {
		v := p.small.Get().(*[]byte)
		return (*v)[:size]
	}
	if size <= MediumSize {
		v := p.medium.Get().(*[]byte)
		return (*v)[:size]
	}
	if size <= LargeSize {
		v := p.large.Get().(*[]byte)
		return (*v)[:size]
	}
	return make([]byte, size)
}

func (p *Pool) Release(data []byte) {
	if data == nil {
		return
	}
	capLen := cap(data)
	switch {
	case capLen <= SmallSize:
		if capLen == SmallSize {
			p.small.Put(&data)
		}
	case capLen <= MediumSize:
		if capLen == MediumSize {
			p.medium.Put(&data)
		}
	case capLen <= LargeSize:
		if capLen == LargeSize {
			p.large.Put(&data)
		}
	}
}
