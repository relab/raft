package raftgorums

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/relab/raft"
)

// Config contains the configuration needed to start an instance of Raft.
type Config struct {
	ID uint64

	Servers []string
	// IDs of server which forms the initial cluster. IDs start at 1, which
	// refers to the first server in Servers.
	InitialCluster []uint64

	Storage raft.Storage

	Batch            bool
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	MaxAppendEntries uint64

	Logger         logrus.FieldLogger
	MetricsEnabled bool
}
