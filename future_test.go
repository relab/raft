package raft_test

import (
	"testing"
	"time"

	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
)

func TestNewPromiseNoFuture(t *testing.T) {
	promise := raft.NewPromiseNoFuture(nil)
	promise.Respond(nil)
}

func TestNewPromiseLogEntryWrite(t *testing.T) {
	var index uint64 = 1
	value := "res"
	entry := &commonpb.Entry{}

	promise, future := raft.NewPromiseEntry(entry)
	logPromise := promise.Write(index)
	logPromise.Respond(value)

	time.Sleep(time.Millisecond)

	select {
	case res := <-future.ResultCh():
		s, ok := res.Value.(string)
		if !ok {
			t.Error("expected type string")
		}
		if s != value {
			t.Errorf("got %s, want %s", s, value)
		}
		if res.Index != index {
			t.Errorf("got %d, want %d", res.Index, index)
		}
	default:
		t.Errorf("expected read from future")
	}

	if logPromise.Duration() < time.Millisecond {
		t.Errorf("expected at least %v duration", time.Millisecond)
	}

	if logPromise.Entry() != entry {
		t.Errorf("got %+v, expected %+v", logPromise.Entry(), entry)
	}
}

func TestNewPromiseLogEntryRead(t *testing.T) {
	var index uint64
	value := "res"
	entry := &commonpb.Entry{}

	promise, future := raft.NewPromiseEntry(entry)
	readPromise := promise.Read()
	readPromise.Respond(value)

	time.Sleep(time.Millisecond)

	select {
	case res := <-future.ResultCh():
		s, ok := res.Value.(string)
		if !ok {
			t.Error("expected type string")
		}
		if s != value {
			t.Errorf("got %s, want %s", s, value)
		}
		if res.Index != index {
			t.Errorf("got %d, want %d", res.Index, index)
		}
	default:
		t.Errorf("expected read from future")
	}

	if readPromise.Duration() < time.Millisecond {
		t.Errorf("expected at least %v duration", time.Millisecond)
	}

	if readPromise.Entry() != entry {
		t.Errorf("got %+v, expected %+v", readPromise.Entry(), entry)
	}
}
