package raftgorums_test

import (
	"reflect"
	"testing"

	"github.com/relab/raft/commonpb"
	"github.com/relab/raft/raftgorums"
)

func TestCacheStorageOneInCache(t *testing.T) {
	storage := raftgorums.NewCacheStorage(newMemory(5, log2()), 10)
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
	storage := raftgorums.NewCacheStorage(newMemory(5, want), 10)
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
	storage := raftgorums.NewCacheStorage(newMemory(5, make(map[uint64]*commonpb.Entry)), 10)

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
