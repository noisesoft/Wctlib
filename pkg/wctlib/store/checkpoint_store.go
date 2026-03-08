package store

import (
	"context"
	"errors"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

type ErrNotFound struct {
	Key string
}

func (e ErrNotFound) Error() string {
	return "checkpoint not found: " + e.Key
}

type CheckpointStore interface {
	Save(ctx context.Context, key string, checkpoint types.StreamCheckpoint) error
	Load(ctx context.Context, key string) (types.StreamCheckpoint, error)
	Delete(ctx context.Context, key string) error
	GC(ctx context.Context, ttl time.Duration) (int, error)
}

var errNoStore = errors.New("checkpoint store is nil")

func Validate(store CheckpointStore) error {
	if store == nil {
		return errNoStore
	}
	return nil
}
