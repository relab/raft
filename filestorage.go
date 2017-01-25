package raft

import (
	"encoding/binary"
	"errors"

	"github.com/boltdb/bolt"
	pb "github.com/relab/raft/raftpb"
)

var (
	stateBucket = []byte("state")
	logBucket   = []byte("log")
)

var ErrKeyNotFound = errors.New("key not found")

type FileStorage struct {
	*bolt.DB

	nextIndex uint64
}

func NewFileStorage(path string, overwrite bool) (*FileStorage, error) {
	db, err := bolt.Open(path, 0600, nil)

	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(true)

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	if overwrite {
		tx.DeleteBucket(stateBucket)
		tx.DeleteBucket(logBucket)
	}

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

func (fs *FileStorage) StoreEntries(entries []*pb.Entry) error {
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

func (fs *FileStorage) GetEntry(index uint64) (*pb.Entry, error) {
	tx, err := fs.Begin(false)

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	bucket := tx.Bucket(logBucket)

	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, index)

	if val := bucket.Get(k); val != nil {
		var entry pb.Entry
		err := entry.Unmarshal(val)

		if err != nil {
			return nil, err
		}

		return &entry, nil
	}

	return nil, ErrKeyNotFound
}

func (fs *FileStorage) GetEntries(from, to uint64) ([]*pb.Entry, error) {
	tx, err := fs.Begin(false)

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	bucket := tx.Bucket(logBucket)

	entries := make([]*pb.Entry, to-from)
	k := make([]byte, 8)

	for i := from; i < to; i++ {
		binary.BigEndian.PutUint64(k, i)

		if val := bucket.Get(k); val != nil {
			var entry pb.Entry
			err := entry.Unmarshal(val)

			if err != nil {
				return nil, err
			}

			entries[i-from] = &entry
		}
	}

	return entries, nil
}

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

func (fs *FileStorage) NumEntries() uint64 {
	return fs.nextIndex
}
