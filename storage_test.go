package raft_test

import (
	"testing"

	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
)

func TestMemory(t *testing.T) {
	var firstIndex, nextIndex uint64 = 1, 2
	entry := &commonpb.Entry{Index: firstIndex}

	m := raft.NewMemory(map[uint64]uint64{
		raft.KeyFirstIndex: firstIndex,
		raft.KeyNextIndex:  nextIndex,
	}, map[uint64]*commonpb.Entry{1: entry})

	// Get key-value.
	first, err := m.Get(raft.KeyFirstIndex)

	if err != nil {
		t.Errorf("got %+v, want no error", err)
	}

	if first != firstIndex {
		t.Errorf("got %d, want %d", first, firstIndex)
	}

	// Get entry that exists.
	e, err := m.GetEntry(firstIndex)

	if err != nil {
		t.Errorf("got %+v, want no error", err)
	}

	if e != entry {
		t.Errorf("got %+v, want %+v", e, entry)
	}

	// Remove entry.
	err = m.RemoveEntries(firstIndex, firstIndex)

	if err != nil {
		t.Errorf("got %+v, want no error", err)
	}

	// Get entry that doesn't exist.
	_, err = m.GetEntry(firstIndex)

	if err == nil {
		t.Errorf("got nil, want error %v", err)
	}

	// Get first/next indexes.
	first, err = m.FirstIndex()
	if err != nil {
		t.Errorf("got %+v, want no error", err)
	}
	next, err := m.NextIndex()
	if err != nil {
		t.Errorf("got %+v, want no error", err)
	}

	if first != firstIndex {
		t.Errorf("got %d, want %d", first, firstIndex)
	}

	// Because we remove one entry.
	if next != firstIndex {
		t.Errorf("got %d, want %d", next, firstIndex)
	}
}
