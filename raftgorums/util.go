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
