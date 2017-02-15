package raft

import (
	"fmt"

	"golang.org/x/net/context"

	commonpb "github.com/relab/raft/raftpb"
)

// ErrNotLeader is returned by Raft when a method is invoked requiring the
// server to be the leader, and it's not. A hint about the actual leader is
// provided.
type ErrNotLeader struct {
	Leader     uint64
	LeaderAddr string
}

func (e ErrNotLeader) Error() string {
	return fmt.Sprintf("not leader, %d is", e.Leader)
}

// Raft represents the interface a Raft node needs expose to the application
// layer.
type Raft interface {
	// ProposeCmd proposes a command. Blocks until Raft handles the message
	// or the context is canceled, i.e., server is busy. Immediately returns
	// an ErrNotLeader error if server isn't the leader.
	ProposeCmd(context.Context, []byte) error

	// Read blocks until Raft has had a successful round of heartbeats in
	// its current term, and the read index is read from Committed, i.e.,
	// Read must not block reads on Committed. A successful call to Read
	// allows read-only queries accumulated up to the point where Read was
	// called, to be safely executed on the state machine. If the context is
	// canceled, i.e., server is busy, and error is returned and Read must
	// be retried. Immediately returns an ErrNotLeader error if server isn't
	// the leader.
	Read(context.Context) error

	// ProposeConf proposes a new configuration. Blocks until Raft handles
	// the message or the context is canceled, i.e., server is busy.
	// Immediately returns an ErrNotLeader error if server isn't the leader.
	ProposeConf(context.Context, TODOConfChange) error

	// Committed returns a stream of committed commands which should be
	// applied to the applications state machine.
	Committed() <-chan []commonpb.Entry
}

type TODOConfChange struct{}
