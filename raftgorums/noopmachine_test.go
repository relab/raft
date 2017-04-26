package raftgorums_test

import (
	"sync/atomic"

	"github.com/relab/raft/commonpb"
)

type noopMachine struct {
	commitIndex uint64
}

func (n *noopMachine) getCommitIndex() uint64 {
	return atomic.LoadUint64(&n.commitIndex)
}

func (n *noopMachine) Apply(entry *commonpb.Entry) interface{} {
	if entry.Index != 0 {
		atomic.StoreUint64(&n.commitIndex, entry.Index)
	}
	return entry
}

func (n *noopMachine) Snapshot() <-chan *commonpb.Snapshot {
	return nil
}

func (n *noopMachine) Restore(*commonpb.Snapshot) {}
