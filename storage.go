package raft

import pb "github.com/relab/libraftgorums/raftpb"

// Keys for indexing term and who was voted for.
const (
	KeyTerm uint64 = iota
	KeyVotedFor
	KeyLogLength
)

// Storage provides an interface for storing and retrieving Raft state.
type Storage interface {
	Set(key uint64, value uint64) error
	Get(key uint64) (uint64, error)

	StoreEntries([]*pb.Entry) error
	GetEntry(index uint64) (*pb.Entry, error)
	GetEntries(from, to uint64) ([]*pb.Entry, error)
	RemoveEntriesFrom(index uint64) error

	NumEntries() uint64
}

// Memory implements the Storage interface as an in-memory storage.
type Memory struct {
	kvstore map[uint64]uint64
	log     []*pb.Entry
}

// NewMemory returns a memory backed storage.
func NewMemory(kvstore map[uint64]uint64, log []*pb.Entry) *Memory {
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
func (m *Memory) StoreEntries(entries []*pb.Entry) error {
	m.log = append(m.log, entries...)
	return nil
}

// GetEntry implements the Storage interface.
func (m *Memory) GetEntry(index uint64) (*pb.Entry, error) {
	return m.log[int(index)], nil
}

// GetEntries implements the Storage interface.
func (m *Memory) GetEntries(from, to uint64) ([]*pb.Entry, error) {
	return m.log[int(from):int(to)], nil
}

// RemoveEntriesFrom implements the Storage interface.
func (m *Memory) RemoveEntriesFrom(index uint64) error {
	m.log = m.log[:index]
	return nil
}

// NumEntries implements the Storage interface.
func (m *Memory) NumEntries() uint64 {
	return uint64(len(m.log))
}
