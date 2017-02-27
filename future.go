package raft

import commonpb "github.com/relab/raft/raftpb"

// Future allows a result to be read after the operation who created it has
// completed.
type Future interface {
	// Must not be called until Result has been read.
	Index() uint64
	Result() <-chan interface{}
}

type EntryFuture struct {
	Entry *commonpb.Entry

	res chan interface{}
}

func NewFuture(entry *commonpb.Entry) *EntryFuture {
	return &EntryFuture{
		Entry: entry,
		res:   make(chan interface{}, 1),
	}
}

func (f *EntryFuture) Index() uint64 {
	return f.Entry.Index
}

func (f *EntryFuture) Result() <-chan interface{} {
	return f.res
}

func (f *EntryFuture) Respond(res interface{}) {
	f.res <- res
}
