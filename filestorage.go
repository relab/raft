package raft

import (
	"encoding/binary"
	"errors"
	"os"

	"github.com/boltdb/bolt"
	"github.com/go-kit/kit/metrics"
	"github.com/relab/raft/commonpb"
)

var (
	stateBucket    = []byte("state")
	logBucket      = []byte("log")
	snapshotBucket = []byte("snapshot")
)

// ErrKeyNotFound means that the given key could not be found in the storage.
var ErrKeyNotFound = errors.New("key not found")

// FileStorage is an implementation of the Storage interface for file based
// storage.
type FileStorage struct {
	*bolt.DB
}

// NewFileStorage returns a new FileStorage using the file given with the path
// argument. Overwrite decides whether to use the database if it already exists
// or overwrite it.
func NewFileStorage(path string, overwrite bool) (*FileStorage, error) {
	// Check if file already exists.
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			// We don't need to overwrite a file that doesn't exist.
			overwrite = false
		} else {
			// If we are unable to verify the existence of the file,
			// there is probably a permission problem.
			return nil, err
		}
	}

	// If overwrite is still true, the file must exist. Thus failing to
	// remove it is an error.
	if overwrite {
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	}

	db, err := bolt.Open(path, 0600, nil)

	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(true)

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	if _, err := tx.CreateBucketIfNotExists(stateBucket); err != nil {
		return nil, err
	}

	if _, err := tx.CreateBucketIfNotExists(logBucket); err != nil {
		return nil, err
	}

	firstIndex := get(tx.Bucket(stateBucket), KeyFirstIndex)
	nextIndex := get(tx.Bucket(stateBucket), KeyNextIndex)

	if firstIndex == 0 {
		err := set(tx.Bucket(stateBucket), KeyFirstIndex, 1)
		if err != nil {
			return nil, err
		}
	}

	if nextIndex == 0 {
		err := set(tx.Bucket(stateBucket), KeyNextIndex, 1)
		if err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &FileStorage{
		DB: db,
	}, nil
}

// Set implements the Storage interface.
func (fs *FileStorage) Set(key uint64, value uint64) error {
	timer := metrics.NewTimer(rmetrics.iowrite)
	defer timer.ObserveDuration()

	tx, err := fs.Begin(true)

	if err != nil {
		return err
	}

	defer tx.Rollback()

	if err := set(tx.Bucket(stateBucket), key, value); err != nil {
		return err
	}

	return tx.Commit()
}

func set(bucket *bolt.Bucket, key uint64, value uint64) error {
	k := make([]byte, 8)
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(k, key)
	binary.BigEndian.PutUint64(v, value)

	return bucket.Put(k, v)
}

// Get implements the Storage interface.
func (fs *FileStorage) Get(key uint64) (uint64, error) {
	timer := metrics.NewTimer(rmetrics.ioread)
	defer timer.ObserveDuration()

	tx, err := fs.Begin(false)

	if err != nil {
		return 0, err
	}

	defer tx.Rollback()

	return get(tx.Bucket(stateBucket), key), nil
}

func get(bucket *bolt.Bucket, key uint64) uint64 {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, key)

	val := bucket.Get(k)

	if val == nil {
		// Default to 0. This lets us get values not yet set.
		return 0
	}

	return binary.BigEndian.Uint64(val)
}

// StoreEntries implements the Storage interface.
func (fs *FileStorage) StoreEntries(entries []*commonpb.Entry) error {
	timer := metrics.NewTimer(rmetrics.iowrite)
	defer timer.ObserveDuration()

	tx, err := fs.Begin(true)

	if err != nil {
		return err
	}

	defer tx.Rollback()

	k := make([]byte, 8)
	bucket := tx.Bucket(logBucket)

	for _, entry := range entries {
		binary.BigEndian.PutUint64(k, entry.Index)

		val, err := entry.Marshal()

		if err != nil {
			return err
		}

		if err := bucket.Put(k, val); err != nil {
			return err
		}
	}

	nextIndex := entries[len(entries)-1].Index + 1
	if err := set(tx.Bucket(stateBucket), KeyNextIndex, nextIndex); err != nil {
		return err
	}

	return tx.Commit()
}

// GetEntry implements the Storage interface.
func (fs *FileStorage) GetEntry(index uint64) (*commonpb.Entry, error) {
	timer := metrics.NewTimer(rmetrics.ioread)
	defer timer.ObserveDuration()

	tx, err := fs.Begin(false)

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	bucket := tx.Bucket(logBucket)

	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, index)

	if val := bucket.Get(k); val != nil {
		var entry commonpb.Entry
		err := entry.Unmarshal(val)

		if err != nil {
			return nil, err
		}

		return &entry, nil
	}

	return nil, ErrKeyNotFound
}

// GetEntries implements the Storage interface.
// TODO We can reduce allocation by passing the slice to fill.
func (fs *FileStorage) GetEntries(first, last uint64) ([]*commonpb.Entry, error) {
	timer := metrics.NewTimer(rmetrics.ioread)
	defer timer.ObserveDuration()

	tx, err := fs.Begin(false)

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	bucket := tx.Bucket(logBucket)

	entries := make([]*commonpb.Entry, last-first+1)
	k := make([]byte, 8)

	i := first
	for j := range entries {
		binary.BigEndian.PutUint64(k, i)

		val := bucket.Get(k)

		if val == nil {
			return nil, ErrKeyNotFound
		}

		var entry commonpb.Entry
		err := entry.Unmarshal(val)

		if err != nil {
			return nil, err
		}

		entries[j] = &entry
		i++

	}

	return entries, nil
}

// RemoveEntries implements the Storage interface.
func (fs *FileStorage) RemoveEntries(first, last uint64) error {
	timer := metrics.NewTimer(rmetrics.iowrite)
	defer timer.ObserveDuration()

	tx, err := fs.Begin(true)

	if err != nil {
		return err
	}

	defer tx.Rollback()

	c := tx.Bucket(logBucket).Cursor()
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, first)
	c.Seek(k)

	for i := first; i <= last; i++ {
		if err := c.Delete(); err != nil {
			return err
		}

		c.Next()
	}

	if err := set(tx.Bucket(stateBucket), KeyNextIndex, first); err != nil {
		return err
	}

	return tx.Commit()
}

// FirstIndex implements the Storage interface.
func (fs *FileStorage) FirstIndex() (uint64, error) {
	return fs.Get(KeyFirstIndex)
}

// NextIndex implements the Storage interface.
func (fs *FileStorage) NextIndex() (uint64, error) {
	return fs.Get(KeyNextIndex)
}

// SetSnapshot implements the Storage interface.
func (fs *FileStorage) SetSnapshot(snapshot *commonpb.Snapshot) error {
	timer := metrics.NewTimer(rmetrics.iowrite)
	defer timer.ObserveDuration()

	tx, err := fs.Begin(true)

	if err != nil {
		return err
	}

	defer tx.Rollback()

	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, KeySnapshot)

	v, err := snapshot.Marshal()

	if err != nil {
		return err
	}

	if err := tx.Bucket(stateBucket).Put(k, v); err != nil {
		return err
	}

	firstIndex := snapshot.LastIncludedIndex
	if err := set(tx.Bucket(stateBucket), KeyFirstIndex, firstIndex); err != nil {
		return err
	}
	nextIndex := firstIndex + 1
	if err := set(tx.Bucket(stateBucket), KeyNextIndex, nextIndex); err != nil {
		return err
	}

	return tx.Commit()
}

// GetSnapshot implements the Storage interface.
func (fs *FileStorage) GetSnapshot() (*commonpb.Snapshot, error) {
	timer := metrics.NewTimer(rmetrics.ioread)
	defer timer.ObserveDuration()

	tx, err := fs.Begin(false)

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, KeySnapshot)

	val := tx.Bucket(stateBucket).Get(k)

	if val != nil {
		var snapshot commonpb.Snapshot
		err := snapshot.Unmarshal(val)

		if err != nil {
			return nil, err
		}

		return &snapshot, nil
	}

	return nil, ErrKeyNotFound
}
