package raftgorums

import (
	"time"

	"github.com/Sirupsen/logrus"
)

// Config contains the configuration needed to start an instance of Raft.
type Config struct {
	ID uint64

	Nodes []string

	Storage Storage

	Batch            bool
	QRPC             bool
	SlowQuorum       bool
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	MaxAppendEntries uint64

	Logger         logrus.FieldLogger
	MetricsEnabled bool
}
