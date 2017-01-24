package raft

import pb "github.com/relab/raft/raftpb"

// Storage provides an interface for storing and retrieving Replica state.
type Storage interface {
	SaveState(term uint64, votedFor uint64) error
	SaveEntries([]*pb.Entry) error
	Load() Persistent
}
