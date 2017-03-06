package raftgorums

import (
	"encoding/binary"
	"errors"
	"os"

	"github.com/boltdb/bolt"
	"github.com/relab/raft/commonpb"
)

var (
	stateBucket = []byte("state")
	logBucket   = []byte("log")
)

// ErrKeyNotFound that the given key could not be found in the storage.
var ErrKeyNotFound = errors.New("key not found")

// FileStorage is an implementation of the Storage interface for file based
// storage.
type FileStorage struct {
	*bolt.DB

	nextIndex uint64
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

	nextIndex := get(tx.Bucket(stateBucket), KeyLogLength)

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &FileStorage{
		DB:        db,
		nextIndex: nextIndex,
	}, nil
}

// Set implements the Storage interface.
func (fs *FileStorage) Set(key uint64, value uint64) error {
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

	if val := bucket.Get(k); val != nil {
		return binary.BigEndian.Uint64(val)
	}

	// Default to 0. This lets us get KeyTerm and KeyVotedFor without having
	// to set them first.
	return 0
}

// StoreEntries implements the Storage interface.
func (fs *FileStorage) StoreEntries(entries []*commonpb.Entry) error {
	tx, err := fs.Begin(true)

	if err != nil {
		return err
	}

	defer tx.Rollback()

	k := make([]byte, 8)
	bucket := tx.Bucket(logBucket)

	for _, entry := range entries {
		binary.BigEndian.PutUint64(k, fs.nextIndex)

		val, err := entry.Marshal()

		if err != nil {
			return err
		}

		if err := bucket.Put(k, val); err != nil {
			return err
		}

		fs.nextIndex++
	}

	if err := set(tx.Bucket(stateBucket), KeyLogLength, fs.nextIndex); err != nil {
		return err
	}

	return tx.Commit()
}

// GetEntry implements the Storage interface.
func (fs *FileStorage) GetEntry(index uint64) (*commonpb.Entry, error) {
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
func (fs *FileStorage) GetEntries(from, to uint64) ([]*commonpb.Entry, error) {
	tx, err := fs.Begin(false)

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	bucket := tx.Bucket(logBucket)

	entries := make([]*commonpb.Entry, to-from)
	k := make([]byte, 8)

	for i := from; i < to; i++ {
		binary.BigEndian.PutUint64(k, i)

		if val := bucket.Get(k); val != nil {
			var entry commonpb.Entry
			err := entry.Unmarshal(val)

			if err != nil {
				return nil, err
			}

			entries[i-from] = &entry
		}
	}

	return entries, nil
}

// RemoveEntriesFrom implements the Storage interface.
func (fs *FileStorage) RemoveEntriesFrom(index uint64) error {
	tx, err := fs.Begin(true)

	if err != nil {
		return err
	}

	defer tx.Rollback()

	c := tx.Bucket(logBucket).Cursor()
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, index)
	c.Seek(k)

	for i := index; i < fs.nextIndex; i++ {
		if err := c.Delete(); err != nil {
			return err
		}

		c.Next()
	}

	fs.nextIndex = index

	if err := set(tx.Bucket(stateBucket), KeyLogLength, fs.nextIndex); err != nil {
		return err
	}

	return tx.Commit()
}

// NumEntries implements the Storage interface.
func (fs *FileStorage) NumEntries() uint64 {
	return fs.nextIndex
}
