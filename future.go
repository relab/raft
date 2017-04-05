package raft

import (
	"time"

	"github.com/relab/raft/commonpb"
)

// Future allows a result to be read after the operation who created it has
// completed.
type Future interface {
	// Must not be called until Result has been read.
	Index() uint64
	Result() <-chan interface{}
}

// EntryFuture implements the Future interface.
type EntryFuture struct {
	Entry *commonpb.Entry

	Created time.Time
	res     chan interface{}
}

// NewFuture initializes and returns a new *EntryFuture.
func NewFuture(entry *commonpb.Entry) *EntryFuture {
	return &EntryFuture{
		Entry:   entry,
		Created: time.Now(),
		res:     make(chan interface{}, 1),
	}
}

// Index implements Future.
func (f *EntryFuture) Index() uint64 {
	return f.Entry.Index
}

// Result implements Future.
func (f *EntryFuture) Result() <-chan interface{} {
	return f.res
}

// Respond stores res on a buffered channel so that it can be consumed by
// reading from Result().
func (f *EntryFuture) Respond(res interface{}) {
	f.res <- res
}

// ConfChangeFuture wraps an EntryFuture. It contains an extra ConfChangeRequest
// field, and the only configuration index on which it can be applied.
type ConfChangeFuture struct {
	Req           *commonpb.ConfChangeRequest
	PrevConfIndex uint64
	*EntryFuture
}
