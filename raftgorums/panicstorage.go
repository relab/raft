package raftgorums

import (
	"github.com/Sirupsen/logrus"
	"github.com/relab/raft/commonpb"
)

// PanicStorage wraps a Storage with methods that panic instead of returning a
// error.
type PanicStorage struct {
	s      Storage
	logger logrus.FieldLogger
}

// Set calls underlying Set method and panics if there is any error.
func (ps *PanicStorage) Set(key uint64, value uint64) {
	err := ps.s.Set(key, value)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"key":   key,
			"value": value,
		}).Panicln("Could not set key-value")
	}
}

// Get calls underlying Get method and panics if there is any error.
func (ps *PanicStorage) Get(key uint64) uint64 {
	value, err := ps.s.Get(key)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"key": key,
		}).Panicln("Could not get value")
	}

	return value
}

// StoreEntries calls underlying StoreEntries method and panics if there is any error.
func (ps *PanicStorage) StoreEntries(entries []*commonpb.Entry) {
	err := ps.s.StoreEntries(entries)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"lenentries": len(entries),
		}).Panicln("Could not store entries")
	}
}

// GetEntry calls underlying GetEntry method and panics if there is any error.
func (ps *PanicStorage) GetEntry(index uint64) *commonpb.Entry {
	entry, err := ps.s.GetEntry(index)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"index": index,
		}).Panicln("Could not get entry")
	}

	return entry
}

// GetEntries calls underlying GetEntries method and panics if there is any error.
func (ps *PanicStorage) GetEntries(first, last uint64) []*commonpb.Entry {
	entries, err := ps.s.GetEntries(first, last)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"first": first,
			"last":  last,
		}).Panicln("Could not get entries")
	}

	return entries
}

// RemoveEntries calls underlying RemoveEntries method and panics if there is any error.
func (ps *PanicStorage) RemoveEntries(first, last uint64) {
	err := ps.s.RemoveEntries(first, last)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"first": first,
			"last":  last,
		}).Panicln("Could not remove entries")
	}
}

// FirstIndex calls underlying FirstIndex method and panics if there is any error.
func (ps *PanicStorage) FirstIndex() uint64 {
	idx, err := ps.s.FirstIndex()

	if err != nil {
		ps.logger.WithError(err).Panicln("Could not get first index")
	}

	return idx
}

// NextIndex calls underlying NextIndex method and panics if there is any error.
func (ps *PanicStorage) NextIndex() uint64 {
	idx, err := ps.s.NextIndex()

	if err != nil {
		ps.logger.WithError(err).Panicln("Could not get next index")
	}

	return idx
}

// SetSnapshot calls underlying SetSnapshot method and panics if there is any error.
func (ps *PanicStorage) SetSnapshot(snapshot *commonpb.Snapshot) {
	err := ps.s.SetSnapshot(snapshot)

	if err != nil {
		ps.logger.WithError(err).WithFields(logrus.Fields{
			"snapshotterm":      snapshot.Term,
			"lastincludedindex": snapshot.LastIncludedIndex,
			"lastincludedterm":  snapshot.LastIncludedTerm,
		}).Panicln("Could not set snapshot")
	}
}

// GetSnapshot calls underlying GetSnapshot method and panics if there is any error.
func (ps *PanicStorage) GetSnapshot() *commonpb.Snapshot {
	snapshot, err := ps.s.GetSnapshot()

	if err != nil {
		ps.logger.WithError(err).Panicln("Could not get snapshot")
	}

	return snapshot
}
