package raft

import (
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/relab/raft/commonpb"
	"github.com/relab/raft/raftgorums"
)

func newFileStorage(t testing.TB, overwrite bool, filepath ...string) (fs *raftgorums.FileStorage, path string, cleanup func()) {
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

	storage, err := raftgorums.NewFileStorage(dbfile, overwrite)

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
	overwrite := false

	// Create storage on path.
	_, path, _ := newFileStorage(t, !overwrite)

	// Recover from path, where file exists.
	_, _, cleanup2 := newFileStorage(t, overwrite, path)
	cleanup2()

	// Recover from path, where file doesn't exist.
	newFileStorage(t, overwrite, path)

	// Overwrite path, where file exists.
	_, _, cleanup3 := newFileStorage(t, !overwrite, path)
	cleanup3()

	// Overwrite path, where file doesn't exist.
	_, _, cleanup4 := newFileStorage(t, !overwrite, path)
	cleanup4()

	if _, err := os.Stat(path); err == nil {
		t.Errorf("got %s exists, want %s removed", path, path)
	}
}

func TestFileStorageStoreValue(t *testing.T) {
	var storage raftgorums.Storage
	storage, _, cleanup := newFileStorage(t, true)
	defer cleanup()

	var expected uint64 = 5

	err := storage.Set(raftgorums.KeyTerm, expected)

	if err != nil {
		t.Fatal(err)
	}

	got, err := storage.Get(raftgorums.KeyTerm)

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, expected) {
		t.Errorf("got %+v, want %+v", got, expected)
	}
}

func TestFileStorageStoreEntry(t *testing.T) {
	var storage raftgorums.Storage
	storage, _, cleanup := newFileStorage(t, true)
	defer cleanup()

	expected := &commonpb.Entry{Term: 5, Index: 1}

	err := storage.StoreEntries([]*commonpb.Entry{expected})

	if err != nil {
		t.Fatal(err)
	}

	got, err := storage.GetEntry(1)

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, expected) {
		t.Errorf("got %+v, want %+v", got, expected)
	}
}

func BenchmarkSnapshot(b *testing.B) {
	storage, _, cleanup := newFileStorage(b, true, "benchsnap.bolt")
	defer cleanup()

	// 200kb.
	data := make([]byte, 200000)
	rand.Read(data)

	snapshot := &commonpb.Snapshot{Data: data}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := storage.SetSnapshot(snapshot); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkThroughput(b *testing.B) {
	rand.Seed(500)

	type benchmark struct {
		name           string
		numEntries     int
		payloadInBytes int
	}

	var benchmarks []benchmark

	for i := 0; i < 5; i++ {
		for _, payload := range []int{10, 50, 100, 200, 500, 1000} {
			numEntries := int(math.Pow(10, float64(i)))
			name := fmt.Sprintf("%d entries with %d bytes", numEntries, payload)
			benchmarks = append(benchmarks, benchmark{
				name,
				numEntries,
				payload,
			})
		}
	}

	for _, bm := range benchmarks {
		storage, _, cleanup := newFileStorage(b, true, "benchthroughput.bolt")
		entries := make([]*commonpb.Entry, bm.numEntries)

		for i := 0; i < bm.numEntries; i++ {
			b := make([]byte, bm.payloadInBytes)
			rand.Read(b)
			entries[i] = &commonpb.Entry{Data: b}
		}

		b.Run("store "+bm.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := storage.StoreEntries(entries); err != nil {
					b.Error(err)
				}
			}
		})

		b.Run("delete "+bm.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := storage.RemoveEntries(0, uint64(len(entries))-1); err != nil {
					b.Error(err)
				}
			}
		})

		cleanup()
	}
}
