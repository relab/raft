package raftgorums

import (
	"math/rand"
	"sort"
	"time"

	gorums "github.com/relab/raft/raftgorums/gorumspb"
)

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}

	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}

	return b
}

func randomTimeout(base time.Duration) time.Duration {
	rnd := time.Duration(rand.Int63()) % base
	return base + rnd
}

// NewQuorumSpec returns a QuorumSpec for len(peers). You need to add 1 if you
// don't include yourself.
func NewQuorumSpec(peers int) gorums.QuorumSpec {
	return &QuorumSpec{
		N: peers - 1,
		Q: peers / 2,
	}
}

// Interval represents an inclusive interval.
type Interval struct {
	Start uint64
	End   uint64
}

// Intervals is a slice of Intervals which implements the sort.Interface
// interface.
type Intervals []*Interval

func (in Intervals) Len() int           { return len(in) }
func (in Intervals) Swap(i, j int)      { in[i], in[j] = in[j], in[i] }
func (in Intervals) Less(i, j int) bool { return in[i].Start < in[j].Start }

// Overlap returns true if this interval overlaps with another.
func (i *Interval) Overlap(j *Interval) bool {
	return i.Start <= j.End
}

// Merge merges two intervals.
func (i *Interval) Merge(j *Interval) {
	i.Start = min(i.Start, j.Start)
	i.End = max(i.End, j.End)
}

// MergeIntervals merges the overlapping intervals.
func MergeIntervals(ints Intervals) []*Interval {
	sort.Sort(sort.Reverse(ints))
	i := 0
	for j := 0; j < len(ints); j++ {
		if i != 0 && ints[i-1].Overlap(ints[j]) {
			for i != 0 && ints[i-1].Overlap(ints[j]) {
				ints[i-1].Merge(ints[j])
				i--
			}
		} else {
			ints[i] = ints[j]
		}
		i++
	}
	return ints[:i]
}

func createIntervals(indexes []uint64, maxEntries uint64) []*Interval {
	ints := make([]*Interval, len(indexes))
	for i := 0; i < len(ints); i++ {
		ints[i] = &Interval{
			Start: indexes[i],
			End:   indexes[i] + maxEntries,
		}
	}
	return MergeIntervals(ints)
}
