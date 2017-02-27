package raft

import commonpb "github.com/relab/raft/raftpb"

// Future allows a result to be read after the operation who created it has
// completed.
type Future interface {
	// Must not be called until Result has been read.
	Index() uint64
	Result() interface{}
}

type EntryFuture struct {
	Entry *commonpb.Entry

	Res  interface{}
	done chan struct{}
}

func NewFuture(entry *commonpb.Entry) *EntryFuture {
	return &EntryFuture{
		Entry: entry,
		done:  make(chan struct{}),
	}
}

func (f *EntryFuture) Index() uint64 {
	return f.Entry.Index
}

func (f *EntryFuture) Result() interface{} {
	<-f.done
	return f.Res
}

func (f *EntryFuture) Respond() {
	close(f.done)
}
