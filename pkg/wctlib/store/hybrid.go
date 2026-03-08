package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

type DedupStore struct {
	inner CheckpointStore
	mu    sync.RWMutex
}

func NewDedupStore(inner CheckpointStore) (*DedupStore, error) {
	if err := Validate(inner); err != nil {
		return nil, err
	}
	return &DedupStore{inner: inner}, nil
}

func (d *DedupStore) Load(ctx context.Context, key string) (types.StreamCheckpoint, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.inner.Load(ctx, key)
}

func (d *DedupStore) Save(ctx context.Context, key string, cp types.StreamCheckpoint) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	existing, err := d.inner.Load(ctx, key)
	if err == nil {
		merged := ReconcileCheckpoints(existing, cp)
		if CheckpointEqual(existing, merged) {
			return nil
		}
		cp = merged
	} else if !isNotFound(err) {
		return err
	}

	return d.inner.Save(ctx, key, cp)
}

func (d *DedupStore) Delete(ctx context.Context, key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.Delete(ctx, key)
}

func (d *DedupStore) GC(ctx context.Context, ttl time.Duration) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inner.GC(ctx, ttl)
}

type FallbackStore struct {
	primary  CheckpointStore
	fallback CheckpointStore
}

func NewFallbackStore(primary, fallback CheckpointStore) (*FallbackStore, error) {
	if err := Validate(primary); err != nil {
		return nil, fmt.Errorf("primary store invalid: %w", err)
	}
	if err := Validate(fallback); err != nil {
		return nil, fmt.Errorf("fallback store invalid: %w", err)
	}
	return &FallbackStore{primary: primary, fallback: fallback}, nil
}

func (f *FallbackStore) Load(ctx context.Context, key string) (types.StreamCheckpoint, error) {
	cp, err := f.primary.Load(ctx, key)
	if err == nil {
		_ = f.fallback.Save(ctx, key, cp)
		return cp, nil
	}
	if !isNotFound(err) {
		return types.StreamCheckpoint{}, err
	}
	cp, fbErr := f.fallback.Load(ctx, key)
	if fbErr != nil {
		return cp, fbErr
	}
	_ = f.primary.Save(ctx, key, cp)
	return cp, nil
}

func (f *FallbackStore) Save(ctx context.Context, key string, cp types.StreamCheckpoint) error {
	primaryErr := f.primary.Save(ctx, key, cp)
	_ = f.fallback.Save(ctx, key, cp)
	return primaryErr
}

func (f *FallbackStore) Delete(ctx context.Context, key string) error {
	fbErr := f.fallback.Delete(ctx, key)
	_ = f.primary.Delete(ctx, key)
	return fbErr
}

func (f *FallbackStore) GC(ctx context.Context, ttl time.Duration) (int, error) {
	primaryRemoved, err := f.primary.GC(ctx, ttl)
	if err != nil {
		return primaryRemoved, err
	}
	fallbackRemoved, _ := f.fallback.GC(ctx, ttl)
	return primaryRemoved + fallbackRemoved, nil
}

func isNotFound(err error) bool {
	var notFound ErrNotFound
	return errors.As(err, &notFound)
}

func ReconcileCheckpoints(existing, incoming types.StreamCheckpoint) types.StreamCheckpoint {
	reconciled := existing
	if incoming.SessionID != "" && existing.SessionID != "" && existing.SessionID != incoming.SessionID {
		return incoming
	}
	if incoming.StreamID != "" && existing.StreamID != "" && existing.StreamID != incoming.StreamID {
		return incoming
	}
	if incoming.LastOffset > existing.LastOffset {
		reconciled.LastOffset = incoming.LastOffset
	}
	if incoming.LastSeq > existing.LastSeq {
		reconciled.LastSeq = incoming.LastSeq
	}
	if incoming.LastAckSequence > reconciled.LastAckSequence {
		reconciled.LastAckSequence = incoming.LastAckSequence
	}
	if incoming.PayloadDigest != "" {
		reconciled.PayloadDigest = incoming.PayloadDigest
	}
	if incoming.ProfileDigest != "" {
		reconciled.ProfileDigest = incoming.ProfileDigest
	}
	reconciled.UpdatedAt = incoming.UpdatedAt
	if reconciled.UpdatedAt.IsZero() || incoming.UpdatedAt.After(reconciled.UpdatedAt) {
		reconciled.UpdatedAt = incoming.UpdatedAt
	}
	if reconciled.StreamID == "" {
		reconciled.StreamID = incoming.StreamID
	}
	if reconciled.SessionID == "" {
		reconciled.SessionID = incoming.SessionID
	}
	return reconciled
}

func CheckpointEqual(left, right types.StreamCheckpoint) bool {
	if left.StreamID != right.StreamID || left.SessionID != right.SessionID {
		return false
	}
	if left.LastOffset != right.LastOffset || left.LastSeq != right.LastSeq {
		return false
	}
	if left.LastAckSequence != right.LastAckSequence {
		return false
	}
	if left.PayloadDigest != right.PayloadDigest || left.ProfileDigest != right.ProfileDigest {
		return false
	}
	return true
}
