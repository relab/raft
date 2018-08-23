package raftgorums

import (
	"container/heap"

	pb "github.com/relab/raft/raftgorums/raftpb"
)

//AEQueue implements the heap interface
type AEQueue []*pb.AppendEntriesRequest

func (aeq AEQueue) Len() int { return len(aeq) }

func (aeq AEQueue) Less(i, j int) bool {
	//This ensures, pop returns the aerequest with the lowest index
	return aeq[i].PrevLogIndex <= aeq[j].PrevLogIndex
}

func (aeq AEQueue) Swap(i, j int) {
	aeq[i], aeq[j] = aeq[j], aeq[i]
}

func (aeq *AEQueue) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*aeq = append(*aeq, x.(*pb.AppendEntriesRequest))
}

func (aeq *AEQueue) Pop() interface{} {
	old := *aeq
	n := len(old)
	x := old[n-1]
	*aeq = old[0 : n-1]
	return x
}

func (aeq *AEQueue) Empty() {
	*aeq = (*aeq)[:0]
	heap.Init(aeq)
}

func InitAEQueue(conf *Config) *AEQueue {
	aeq := make(AEQueue, 0, conf.MaxAEBuffer)
	//I wonder if this is unnecessary, since the heap is empty.
	heap.Init(&aeq)
	return &aeq
}

//enque saves an out of order AppendEntriesRequest and checks if a catchup should be sent
func (r Raft) enque(aer *pb.AppendEntriesRequest) (catchUp bool) {
	if r.appendEntryQueue.Len() == r.maxaebuffer {
		r.appendEntryQueue.Empty()
		return true
	}
	heap.Push(r.appendEntryQueue, aer)
	if aer.CommitIndex > r.commitIndex+r.maxMissingCommit {
		return true
	}
	return false
}

func (r Raft) tryEnqued() {
	success := true
	for r.appendEntryQueue.Len() > 0 && success {
		success = r.handleAppendEntriesRequest((*r.appendEntryQueue)[0]).Success
		if success {
			heap.Pop(r.appendEntryQueue)
		}
	}
}
