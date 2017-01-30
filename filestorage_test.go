package raft_test

import (
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/relab/libraftgorums"
	pb "github.com/relab/libraftgorums/raftpb"
)

func newFileStorage(t *testing.T) *raft.FileStorage {
	file, err := ioutil.TempFile("", "bolt")

	if err != nil {
		t.Fatal(err)
	}

	storage, err := raft.NewFileStorage(file.Name(), true)

	if err != nil {
		t.Fatal(err)
	}

	return storage
}

func TestFileStorageStoreValue(t *testing.T) {
	var storage raft.Storage = newFileStorage(t)
	var expected uint64 = 5

	err := storage.Set(raft.KeyTerm, expected)

	if err != nil {
		t.Fatal(err)
	}

	got, err := storage.Get(raft.KeyTerm)

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, expected) {
		t.Errorf("got %+v, want %+v", got, expected)
	}
}

func TestFileStorageStoreEntry(t *testing.T) {
	var storage raft.Storage = newFileStorage(t)
	expected := &pb.Entry{Term: 5}

	err := storage.StoreEntries([]*pb.Entry{expected})

	if err != nil {
		t.Fatal(err)
	}

	got, err := storage.GetEntry(0)

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, expected) {
		t.Errorf("got %+v, want %+v", got, expected)
	}
}
