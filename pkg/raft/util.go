package raft

import (
	"math/rand"
	"time"
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
	return time.Duration(rand.Int63n(base.Nanoseconds()) + base.Nanoseconds())
}

func newQuorumSpec(peers int) *QuorumSpec {
	n := peers + 1

	return &QuorumSpec{
		N: n - 1,
		Q: n / 2,
	}
}
