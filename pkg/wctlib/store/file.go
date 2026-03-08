package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/noisesoft/Wctlib/pkg/wctlib/types"
)

type fileCheckpointStore struct {
	dir string
}

func NewFileCheckpointStore(dir string) (CheckpointStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("store path is empty")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	return &fileCheckpointStore{dir: dir}, nil
}

func (f *fileCheckpointStore) keyPath(key string) string {
	h := 2166136261
	for _, ch := range key {
		h ^= int(ch)
		h *= 16777619
	}
	name := strconv.Itoa(h)
	return filepath.Join(f.dir, fmt.Sprintf("%s.json", name))
}

func (f *fileCheckpointStore) Save(_ context.Context, key string, checkpoint types.StreamCheckpoint) error {
	path := f.keyPath(key)
	payload, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o640); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func (f *fileCheckpointStore) Load(_ context.Context, key string) (types.StreamCheckpoint, error) {
	path := f.keyPath(key)
	payload, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return types.StreamCheckpoint{}, ErrNotFound{Key: key}
		}
		return types.StreamCheckpoint{}, err
	}
	var checkpoint types.StreamCheckpoint
	if err := json.Unmarshal(payload, &checkpoint); err != nil {
		return types.StreamCheckpoint{}, err
	}
	return checkpoint, nil
}

func (f *fileCheckpointStore) Delete(_ context.Context, key string) error {
	return os.Remove(f.keyPath(key))
}

func (f *fileCheckpointStore) GC(_ context.Context, ttl time.Duration) (int, error) {
	if ttl <= 0 {
		return 0, nil
	}
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return 0, err
	}
	limit := time.Now().UTC().Add(-ttl)
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(f.dir, entry.Name())
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		if info.ModTime().Before(limit) {
			if err := os.Remove(path); err == nil {
				removed++
			}
		}
	}
	return removed, nil
}
