package raft

import (
	"fmt"

	"github.com/relab/raft/commonpb"

	"golang.org/x/net/context"
)

// NOOP is used in the data field for a no-op entry.
var NOOP = []byte("noop")

// ErrNotLeader is returned by Raft when a method is invoked requiring the
// server to be the leader, and it's not. A hint about the actual leader is
// provided.
type ErrNotLeader struct {
	Leader uint64
}

func (e ErrNotLeader) Error() string {
	return fmt.Sprintf("not leader, %d is", e.Leader)
}

// Raft represents the interface a Raft node needs expose to the application
// layer.
type Raft interface {
	// ProposeCmd proposes a command. Blocks until Raft handles the message
	// or the context is canceled, i.e., server is busy. Immediately returns
	// an ErrNotLeader error if server isn't the leader. If everything works
	// out the command will be applied to the state machine and the result
	// available through the future returned.
	ProposeCmd(context.Context, []byte) (Future, error)

	// ReadCmd works the same way as ProposeCmd but does not write any
	// entries to the log.
	ReadCmd(context.Context, []byte) (Future, error)

	// ProposeConf proposes a new configuration. Behaves as ProposeCmd.
	ProposeConf(context.Context, *commonpb.ConfChangeRequest) (Future, error)
}
