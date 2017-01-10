package raft

import (
	"hash/fnv"
	"math/rand"
	"net"
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

func lookupTables(id uint64, raftOrder []string, nodeOrder []uint32) (map[uint32]uint64, map[uint64]uint32, error) {
	h := fnv.New32a()

	raftID := make(map[uint32]uint64)
	nodeID := make(map[uint64]uint32)

	for _, id := range nodeOrder {
		raftID[id] = 0
	}

	var rid int

	for _, addr := range raftOrder {
		// Increase id to compensate gap.
		if uint64(rid+1) == id {
			rid++
		}

		tcpAddr, err := net.ResolveTCPAddr("tcp", addr)

		if err != nil {
			return nil, nil, err
		}

		_, _ = h.Write([]byte(tcpAddr.String()))
		nid := h.Sum32()
		h.Reset()

		raftID[nid] = uint64(rid + 1)
		nodeID[uint64(rid+1)] = nid

		rid++
	}

	return raftID, nodeID, nil
}

func randomTimeout(base time.Duration) time.Duration {
	return time.Duration(rand.Int63n(base.Nanoseconds()) + base.Nanoseconds())
}

func newQuorumSpec(peers int, slow bool) *QuorumSpec {
	n := peers + 1
	sq := n / 2

	if slow {
		sq = n - 1
	}

	return &QuorumSpec{
		N:  n,
		SQ: sq,
		FQ: n / 2,
	}
}
