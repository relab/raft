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

func NewQuorumSpec(peers int) *QuorumSpec {
	return &QuorumSpec{
		N: peers - 1,
		Q: peers / 2,
	}
}
