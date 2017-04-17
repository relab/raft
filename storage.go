package raft

import (
	"errors"

	"github.com/relab/raft/commonpb"
)

// Keys for indexing term and who was voted for.
const (
	KeyTerm uint64 = iota
	KeyVotedFor
	KeyFirstIndex
	KeyNextIndex
	KeySnapshot
)

// Storage provides an interface for storing and retrieving Raft state.
type Storage interface {
	Set(key uint64, value uint64) error
	Get(key uint64) (uint64, error)

	// Entries must be stored such that Entry.Index can be used to retrieve
	// that entry in the future.
	StoreEntries([]*commonpb.Entry) error
	// Retrieves entry with Entry.Index == index.
	GetEntry(index uint64) (*commonpb.Entry, error)
	// Get the inclusive range of entries from first to last.
	GetEntries(first, last uint64) ([]*commonpb.Entry, error)
	// Remove the inclusive range of entries from first to last.
	RemoveEntries(first, last uint64) error

	// Should return 1 if not set.
	FirstIndex() (uint64, error)
	// Should return 1 if not set.
	NextIndex() (uint64, error)

	SetSnapshot(*commonpb.Snapshot) error
	GetSnapshot() (*commonpb.Snapshot, error)
}

// TODO Create LogStore wrapper.

// Memory implements the Storage interface as an in-memory storage.
type Memory struct {
	kvstore map[uint64]uint64
	log     map[uint64]*commonpb.Entry
}

// NewMemory returns a memory backed storage.
func NewMemory(kvstore map[uint64]uint64, log map[uint64]*commonpb.Entry) *Memory {
	if _, ok := kvstore[KeyFirstIndex]; !ok {
		kvstore[KeyFirstIndex] = 1
	}
	if _, ok := kvstore[KeyNextIndex]; !ok {
		kvstore[KeyNextIndex] = 1
	}
	return &Memory{
		kvstore: kvstore,
		log:     log,
	}
}

// Set implements the Storage interface.
func (m *Memory) Set(key, value uint64) error {
	m.kvstore[key] = value
	return nil
}

// Get implements the Storage interface.
func (m *Memory) Get(key uint64) (uint64, error) {
	return m.kvstore[key], nil
}

// StoreEntries implements the Storage interface.
func (m *Memory) StoreEntries(entries []*commonpb.Entry) error {
	i := m.kvstore[KeyNextIndex]
	for _, entry := range entries {
		m.log[i] = entry
		i++
	}
	return m.Set(KeyNextIndex, i)
}

// GetEntry implements the Storage interface.
func (m *Memory) GetEntry(index uint64) (*commonpb.Entry, error) {
	entry, ok := m.log[index]

	if !ok {
		return nil, ErrKeyNotFound
	}

	return entry, nil
}

// GetEntries implements the Storage interface.
func (m *Memory) GetEntries(first, last uint64) ([]*commonpb.Entry, error) {
	entries := make([]*commonpb.Entry, last-first+1)

	i := first
	for j := range entries {
		entries[j] = m.log[i]
		i++
	}

	return entries, nil
}

// RemoveEntries implements the Storage interface.
func (m *Memory) RemoveEntries(first, last uint64) error {
	for i := first; i <= last; i++ {
		delete(m.log, i)
	}

	return m.Set(KeyNextIndex, first)
}

// FirstIndex implements the Storage interface.
func (m *Memory) FirstIndex() (uint64, error) {
	first := m.kvstore[KeyFirstIndex]
	return first, nil
}

// NextIndex implements the Storage interface.
func (m *Memory) NextIndex() (uint64, error) {
	next := m.kvstore[KeyNextIndex]
	return next, nil
}

// SetSnapshot implements the Storage interface.
func (m *Memory) SetSnapshot(*commonpb.Snapshot) error {
	return nil
}

// GetSnapshot implements the Storage interface.
func (m *Memory) GetSnapshot() (*commonpb.Snapshot, error) {
	return nil, errors.New("not implemented")
}
