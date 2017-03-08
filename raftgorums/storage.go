package raftgorums

import (
	"errors"
	"math"

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

	StoreEntries([]*commonpb.Entry) error
	GetEntry(index uint64) (*commonpb.Entry, error)
	// Inclusive first, exclusive last.
	GetEntries(first, last uint64) ([]*commonpb.Entry, error)
	// Inclusive.
	RemoveEntries(first, last uint64) error

	// FirstIndex and NextIndex should be saved to memory when creating a
	// new storage. When they are modified on disk, the should be updated in
	// memory.
	FirstIndex() uint64
	NextIndex() uint64

	SetSnapshot(*commonpb.Snapshot) error
	GetSnapshot() (*commonpb.Snapshot, error)
}

// Memory implements the Storage interface as an in-memory storage.
type Memory struct {
	kvstore map[uint64]uint64
	log     map[uint64]*commonpb.Entry
}

// NewMemory returns a memory backed storage.
func NewMemory(kvstore map[uint64]uint64, log map[uint64]*commonpb.Entry) *Memory {
	var first uint64

	if len(log) > 0 {
		first = math.MaxUint64
	}

	for i := range log {
		if i < first {
			first = i
		}
	}

	kvstore[KeyFirstIndex] = first
	kvstore[KeyNextIndex] = first + uint64(len(log))

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
	i := m.NextIndex() + 1
	for _, entry := range entries {
		m.log[i] = entry
		i++
	}

	err := m.Set(KeyNextIndex, i)
	return err
}

// GetEntry implements the Storage interface.
func (m *Memory) GetEntry(index uint64) (*commonpb.Entry, error) {
	return m.log[index], nil
}

// GetEntries implements the Storage interface.
func (m *Memory) GetEntries(first, last uint64) ([]*commonpb.Entry, error) {
	entries := make([]*commonpb.Entry, last-first)

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

	err := m.Set(KeyFirstIndex, m.log[last+1].Index)
	return err
}

// FirstIndex implements the Storage interface.
func (m *Memory) FirstIndex() uint64 {
	first, _ := m.Get(KeyFirstIndex)
	return first
}

// NextIndex implements the Storage interface.
func (m *Memory) NextIndex() uint64 {
	next, _ := m.Get(KeyNextIndex)
	return next
}

// SetSnapshot implements the Storage interface.
func (m *Memory) SetSnapshot(*commonpb.Snapshot) error {
	return nil
}

// GetSnapshot implements the Storage interface.
func (m *Memory) GetSnapshot() (*commonpb.Snapshot, error) {
	return nil, errors.New("not implemented")
}
