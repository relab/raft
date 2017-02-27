package raft

import commonpb "github.com/relab/raft/raftpb"

type StateMachine interface {
	Apply(*commonpb.Entry) interface{}
}
