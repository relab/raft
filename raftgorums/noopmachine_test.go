package raftgorums_test

import "github.com/relab/raft/commonpb"

type noopMachine struct{}

func (n *noopMachine) Apply(*commonpb.Entry) interface{} {
	return nil
}

func (n *noopMachine) Snapshot() <-chan *commonpb.Snapshot {
	return nil
}

func (n *noopMachine) Restore(*commonpb.Snapshot) {}
