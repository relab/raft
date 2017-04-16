package raftgorums

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
	rnd := time.Duration(rand.Int63()) % base
	return base + rnd
}

// NewQuorumSpec returns a QuorumSpec for len(peers). You need to add 1 if you
// don't include yourself.
func NewQuorumSpec(peers int) *QuorumSpec {
	return &QuorumSpec{
		N: peers - 1,
		Q: peers / 2,
	}
}
