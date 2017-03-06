package raft

import "github.com/relab/raft/commonpb"

// StateMachine provides an interface for state machines using the Raft log.
// Raft will not call any of these methods concurrently, i.e., your StateMachine
// implementation does not need to be thread-safe.
type StateMachine interface {
	Apply(*commonpb.Entry) interface{}

	Snapshot() *commonpb.Snapshot
	Restore(*commonpb.Snapshot)
}
