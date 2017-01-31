package raft_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/relab/libraftgorums"
	pb "github.com/relab/libraftgorums/raftpb"
)

func newFileStorage(t *testing.T, overwrite bool, filepath ...string) (fs *raft.FileStorage, path string, cleanup func()) {
	var dbfile string

	if len(filepath) < 1 {
		file, err := ioutil.TempFile("", "bolt")

		if err != nil {
			t.Fatal(err)
		}

		dbfile = file.Name()
	} else {
		dbfile = filepath[0]
	}

	storage, err := raft.NewFileStorage(dbfile, overwrite)

	if err != nil {
		t.Fatal(err)
	}

	return storage, dbfile, func() {
		if err := os.Remove(dbfile); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNewFileStorage(t *testing.T) {
	recover := false

	// Create storage on path.
	_, path, _ := newFileStorage(t, !recover)

	// Recover from path, where file exists.
	_, _, cleanup2 := newFileStorage(t, recover, path)
	cleanup2()

	// Recover from path, where file doesn't exist.
	newFileStorage(t, recover, path)

	// Overwrite path, where file exists.
	_, _, cleanup3 := newFileStorage(t, !recover, path)
	cleanup3()

	// Overwrite path, where file doesn't exist.
	_, _, cleanup4 := newFileStorage(t, !recover, path)
	cleanup4()

	if _, err := os.Stat(path); err == nil {
		t.Errorf("got %s exists, want %s removed", path, path)
	}
}

func TestFileStorageStoreValue(t *testing.T) {
	var storage raft.Storage
	storage, _, cleanup := newFileStorage(t, true)
	defer cleanup()

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
	var storage raft.Storage
	storage, _, cleanup := newFileStorage(t, true)
	defer cleanup()

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
