package raftgorums

import (
	"errors"

	"github.com/Sirupsen/logrus"
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

	// FirstIndex and NextIndex should be saved to memory when creating a
	// new storage. When they are modified on disk, the should be updated in
	// memory. TODO We should create a StorageCache wrapper eventually that
	// gives this behavior.

	// Should return 1 if not set.
	FirstIndex() uint64
	// Should return 1 if not set.
	NextIndex() uint64

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
	i := m.NextIndex()
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

type panicStorage struct {
	s      Storage
	logger logrus.FieldLogger
}

func (ps *panicStorage) Set(key uint64, value uint64) {
	err := ps.s.Set(key, value)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"key":   key,
			"value": value,
		}).Panicln("Could not set key-value")
	}
}

func (ps *panicStorage) Get(key uint64) uint64 {
	value, err := ps.s.Get(key)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"key": key,
		}).Panicln("Could not get value")
	}

	return value
}

func (ps *panicStorage) StoreEntries(entries []*commonpb.Entry) {
	err := ps.s.StoreEntries(entries)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"lenentries": len(entries),
		}).Panicln("Could not store entries")
	}
}

func (ps *panicStorage) GetEntry(index uint64) *commonpb.Entry {
	entry, err := ps.s.GetEntry(index)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"index": index,
		}).Panicln("Could not get entry")
	}

	return entry
}

func (ps *panicStorage) GetEntries(first, last uint64) []*commonpb.Entry {
	entries, err := ps.s.GetEntries(first, last)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"first": first,
			"last":  last,
		}).Panicln("Could not get entries")
	}

	return entries
}

func (ps *panicStorage) RemoveEntries(first, last uint64) {
	err := ps.s.RemoveEntries(first, last)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"first": first,
			"last":  last,
		}).Panicln("Could not remove entries")
	}
}

func (ps *panicStorage) FirstIndex() uint64 {
	return ps.s.FirstIndex()
}

func (ps *panicStorage) NextIndex() uint64 {
	return ps.s.NextIndex()
}

func (ps *panicStorage) SetSnapshot(snapshot *commonpb.Snapshot) {
	err := ps.s.SetSnapshot(snapshot)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"snapshotterm":      snapshot.Term,
			"lastincludedindex": snapshot.LastIncludedIndex,
			"lastincludedterm":  snapshot.LastIncludedTerm,
		}).Panicln("Could not set snapshot")
	}
}

func (ps *panicStorage) GetSnapshot() *commonpb.Snapshot {
	snapshot, err := ps.s.GetSnapshot()

	if err != nil {
		ps.logger.WithError(err).Panicln("Could not get snapshot")
	}

	return snapshot
}
