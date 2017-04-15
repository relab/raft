package raft

import (
	"reflect"
	"testing"

	"github.com/relab/raft/commonpb"
)

// None represents no server.
const none = 0

func log2() map[uint64]*commonpb.Entry {
	return map[uint64]*commonpb.Entry{
		1: {
			Index: 1,
			Term:  4,
			Data:  []byte("first"),
		},
		2: {
			Index: 2,
			Term:  5,
			Data:  []byte("second"),
		},
	}
}

// TODO Change to: currentTerm uint64, votedFor uint64, l map[uint64]*commonpb.Entry
func newMemory(t uint64, l map[uint64]*commonpb.Entry) *Memory {
	return NewMemory(map[uint64]uint64{
		KeyTerm:      t,
		KeyVotedFor:  none,
		KeyNextIndex: uint64(len(l) + 1),
	}, l)
}

func logPlusEntry(l map[uint64]*commonpb.Entry, entry *commonpb.Entry) map[uint64]*commonpb.Entry {
	nl := make(map[uint64]*commonpb.Entry)

	for k, v := range l {
		nl[k] = v
	}

	nl[entry.Index] = entry

	return nl
}

func TestCacheStorageOneInCache(t *testing.T) {
	storage := NewCacheStorage(newMemory(5, log2()), 10)
	entry := &commonpb.Entry{Index: 3}
	err := storage.StoreEntries([]*commonpb.Entry{entry})

	if err != nil {
		t.Error(err)
	}

	entries, err := storage.GetEntries(1, 3)

	if err != nil {
		t.Error(err)
	}

	got := make(map[uint64]*commonpb.Entry)

	for _, entry := range entries {
		got[entry.Index] = entry
	}

	want := logPlusEntry(log2(), entry)

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestCacheStorageNoneInCache(t *testing.T) {
	entry := &commonpb.Entry{Index: 3}
	want := logPlusEntry(log2(), entry)
	storage := NewCacheStorage(newMemory(5, want), 10)
	entries, err := storage.GetEntries(1, 3)

	if err != nil {
		t.Error(err)
	}

	got := make(map[uint64]*commonpb.Entry)

	for _, entry := range entries {
		got[entry.Index] = entry
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestCacheStorageAllInCache(t *testing.T) {
	storage := NewCacheStorage(newMemory(5, make(map[uint64]*commonpb.Entry)), 10)

	var log2Slice []*commonpb.Entry
	for _, entry := range log2() {
		log2Slice = append(log2Slice, entry)
	}
	err := storage.StoreEntries(log2Slice)

	if err != nil {
		t.Error(err)
	}

	entries, err := storage.GetEntries(1, 2)

	if err != nil {
		t.Error(err)
	}

	got := make(map[uint64]*commonpb.Entry)

	for _, entry := range entries {
		got[entry.Index] = entry
	}

	if !reflect.DeepEqual(got, log2()) {
		t.Errorf("got %+v, want %+v", got, log2())
	}
}
