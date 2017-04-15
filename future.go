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

type PromiseEntry interface {
	Write(uint64) PromiseLogEntry
	Read() PromiseLogEntry
	Respond(interface{})
}

type PromiseLogEntry interface {
	Entry() *commonpb.Entry
	Duration() time.Duration
	Respond(interface{})
}

func NewPromiseNoFuture(entry *commonpb.Entry) PromiseLogEntry {
	return &promiseEntry{
		entry:   entry,
		created: time.Now(),
	}
}

func NewPromiseLogEntry(entry *commonpb.Entry) (PromiseEntry, Future) {
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

func (f *promiseEntry) Write(index uint64) PromiseLogEntry {
	f.entry.Index = index
	return f
}

func (f *promiseEntry) Read() PromiseLogEntry {
	return f
}

func (f *promiseEntry) Entry() *commonpb.Entry {
	return f.entry
}

func (f *promiseEntry) Duration() time.Duration {
	return time.Since(f.created)
}

func (f *promiseEntry) Respond(value interface{}) {
	if f.res == nil {
		return
	}
	f.res <- Result{f.entry.Index, value}
}

func (f *promiseEntry) ResultCh() <-chan Result {
	return f.res
}
