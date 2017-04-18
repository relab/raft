package raftgorums_test

import "github.com/relab/raft/commonpb"

type noopMachine struct{}

func (n *noopMachine) Apply(entry *commonpb.Entry) interface{} {
	return entry
}

func (n *noopMachine) Snapshot() <-chan *commonpb.Snapshot {
	return nil
}

func (n *noopMachine) Restore(*commonpb.Snapshot) {}
