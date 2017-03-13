package raftgorums

import (
	"sync"

	"github.com/relab/raft/commonpb"
)

// CacheStorage wraps a Storage adding a layer of caching. It uses the
// underlying storage as a fallback if the data is not cached.
type CacheStorage struct {
	s Storage

	l          sync.RWMutex
	stateCache map[uint64]uint64
	logCache   []*commonpb.Entry
}

// NewCacheStorage returns a new initialized CacheStorage.
func NewCacheStorage(s Storage, cacheSize int) *CacheStorage {
	return &CacheStorage{
		s:          s,
		stateCache: make(map[uint64]uint64),
		logCache:   make([]*commonpb.Entry, cacheSize),
	}
}

// Set implements the Storage interface.
func (cs *CacheStorage) Set(key uint64, value uint64) error {
	cs.l.Lock()
	cs.stateCache[key] = value
	cs.l.Unlock()
	return cs.s.Set(key, value)
}

// Get implements the Storage interface.
func (cs *CacheStorage) Get(key uint64) (uint64, error) {
	cs.l.Lock()
	defer cs.l.Unlock()

	value, ok := cs.stateCache[key]
	if !ok {
		return cs.s.Get(key)
	}
	return value, nil
}

// StoreEntries implements the Storage interface.
func (cs *CacheStorage) StoreEntries(entries []*commonpb.Entry) error {
	cs.l.Lock()
	for _, entry := range entries {
		cs.logCache[entry.Index%uint64(len(cs.logCache))] = entry
	}
	cs.stateCache[KeyNextIndex] = entries[len(entries)-1].Index + 1
	cs.l.Unlock()
	return cs.s.StoreEntries(entries)
}

// GetEntry implements the Storage interface.
func (cs *CacheStorage) GetEntry(index uint64) (*commonpb.Entry, error) {
	cs.l.RLock()
	entry := cs.logCache[index%uint64(len(cs.logCache))]
	cs.l.RUnlock()

	if entry != nil && entry.Index == index {
		return entry, nil
	}

	return cs.s.GetEntry(index)
}

// GetEntries implements the Storage interface.
func (cs *CacheStorage) GetEntries(first, last uint64) ([]*commonpb.Entry, error) {
	entries := make([]*commonpb.Entry, last-first+1)

	cs.l.RLock()
	index := last
	for {
		entry := cs.logCache[index%uint64(len(cs.logCache))]

		if entry == nil || entry.Index != index {
			break
		}

		entries[entry.Index-first] = entry

		if index == first {
			break
		}

		index--
	}
	cs.l.RUnlock()

	if index == first {
		return entries, nil
	}

	prefix, err := cs.s.GetEntries(first, index)

	if err != nil {
		return nil, err
	}

	for i := 0; i < len(prefix); i++ {
		entries[i] = prefix[i]
	}

	return entries, nil
}

// RemoveEntries implements the Storage interface.
func (cs *CacheStorage) RemoveEntries(first, last uint64) error {
	cs.l.Lock()
	cs.stateCache = make(map[uint64]uint64)
	cs.logCache = make([]*commonpb.Entry, len(cs.logCache))
	cs.l.Unlock()

	return cs.s.RemoveEntries(first, last)
}

// FirstIndex implements the Storage interface.
func (cs *CacheStorage) FirstIndex() (uint64, error) {
	return cs.Get(KeyFirstIndex)
}

// NextIndex implements the Storage interface.
func (cs *CacheStorage) NextIndex() (uint64, error) {
	return cs.Get(KeyNextIndex)
}

// SetSnapshot implements the Storage interface.
func (cs *CacheStorage) SetSnapshot(snapshot *commonpb.Snapshot) error {
	return cs.s.SetSnapshot(snapshot)
}

// GetSnapshot implements the Storage interface.
func (cs *CacheStorage) GetSnapshot() (*commonpb.Snapshot, error) {
	return cs.s.GetSnapshot()
}
