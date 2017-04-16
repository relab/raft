package raft

import (
	"time"

	"github.com/relab/raft/commonpb"
)

// Result contains the index of the committed entry and the accompanied
// response.
type Result struct {
	Index uint64
	Value interface{}
}

// Future allows a result to be read after the operation who created it has
// completed.
type Future interface {
	ResultCh() <-chan Result
}

// PromiseEntry is a promise to either write some entry to the log, or read some
// result from the state machine. Invoking Write turns it into a promise to
// write to the log, invoking read turns it into the other. Respond can be used
// to respond early, if we cannot proceed with the request. Respond is
// non-blocking but must only be invoked once.
type PromiseEntry interface {
	Write(uint64) PromiseLogEntry
	Read() PromiseLogEntry
	Respond(interface{})
}

// PromiseLogEntry is a promise to execute some request, usually committing an
// entry to the log. Respond is used to inform a listener (Future) about the
// result of the promise. Respond is non-blocking but must only be invoked once.
type PromiseLogEntry interface {
	Entry() *commonpb.Entry
	Duration() time.Duration
	Respond(interface{})
}

// NewPromiseNoFuture returns a struct implementing the PromiseLogEntry but does
// nothing when Respond is called.
func NewPromiseNoFuture(entry *commonpb.Entry) PromiseLogEntry {
	return &promiseEntry{
		entry:   entry,
		created: time.Now(),
	}
}

// NewPromiseEntry returns a PromiseEntry and a Future which can be used to get
// the response from the promise at a later time.
func NewPromiseEntry(entry *commonpb.Entry) (PromiseEntry, Future) {
	promise := &promiseEntry{
		entry:   entry,
		created: time.Now(),
		res:     make(chan Result, 1),
	}

	return promise, promise
}

type promiseEntry struct {
	entry   *commonpb.Entry
	created time.Time
	res     chan Result
}

// Write implements the PromiseEntry interface.
func (f *promiseEntry) Write(index uint64) PromiseLogEntry {
	f.entry.Index = index
	return f
}

// Read implements the PromiseEntry interface.
func (f *promiseEntry) Read() PromiseLogEntry {
	return f
}

// Entry implements PromiseLogEntry interface.
func (f *promiseEntry) Entry() *commonpb.Entry {
	return f.entry
}

// Duration implements PromiseLogEntry interface.
func (f *promiseEntry) Duration() time.Duration {
	return time.Since(f.created)
}

// Respond implements PromiseEntry and PromiseLogEntry interface.
func (f *promiseEntry) Respond(value interface{}) {
	if f.res == nil {
		return
	}
	f.res <- Result{f.entry.Index, value}
}

// ResultCh implements Future interface.
func (f *promiseEntry) ResultCh() <-chan Result {
	return f.res
}
