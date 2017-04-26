package raftgorums

import (
	"errors"
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

	EntriesPerMsg uint64
	// CatchupMultiplier is how many times more EntriesPerMsg we are allowed
	// to use when doing a catch up, i.e., EntriesPerMsg*CatchupMultiplier.
	CatchupMultiplier uint64

	Logger         logrus.FieldLogger
	MetricsEnabled bool
}

func validate(cfg *Config) error {
	if cfg.ID == 0 {
		return errors.New("invalid id")
	}

	if cfg.Logger == nil {
		cfg.Logger = logrus.New()
	}

	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 50 * time.Millisecond
	}

	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 250 * time.Millisecond
	}

	if cfg.EntriesPerMsg == 0 {
		cfg.EntriesPerMsg = 64
	}

	if cfg.CatchupMultiplier == 0 {
		cfg.CatchupMultiplier = 160
	}

	return nil
}
