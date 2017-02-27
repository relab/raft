package raft

import commonpb "github.com/relab/raft/raftpb"

// StateMachine provides an interface for state machines using the Raft log.
type StateMachine interface {
	Apply(*commonpb.Entry) interface{}
}
