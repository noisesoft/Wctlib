package store

import (
	"context"
	"sync"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

type memoryEntry struct {
	checkpoint types.StreamCheckpoint
	savedAt    time.Time
}

type memoryCheckpointStore struct {
	mu   sync.RWMutex
	data map[string]memoryEntry
}

func NewMemoryStore() CheckpointStore {
	return &memoryCheckpointStore{data: map[string]memoryEntry{}}
}

func (m *memoryCheckpointStore) Save(_ context.Context, key string, checkpoint types.StreamCheckpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = memoryEntry{checkpoint: checkpoint, savedAt: time.Now().UTC()}
	return nil
}

func (m *memoryCheckpointStore) Load(_ context.Context, key string) (types.StreamCheckpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.data[key]
	if !ok {
		return types.StreamCheckpoint{}, ErrNotFound{Key: key}
	}
	return entry.checkpoint, nil
}

func (m *memoryCheckpointStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *memoryCheckpointStore) GC(_ context.Context, ttl time.Duration) (int, error) {
	if ttl <= 0 {
		return 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	limit := time.Now().UTC().Add(-ttl)
	removed := 0
	for key, entry := range m.data {
		if entry.savedAt.Before(limit) {
			delete(m.data, key)
			removed++
		}
	}
	return removed, nil
}
