package main

import (
	"errors"
	"flag"
	"log"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/relab/raft"
	"github.com/relab/raft/debug"
	"github.com/relab/raft/proto/gorums"
)

// Errors
var (
	ErrRegisterClient = errors.New("Could not register client with leader")
	ErrClientCommand  = errors.New("Could not send command to leader")
	ErrSessionExpired = errors.New("Client session has expired")
)

// MaxAttempts sets the maximum number of errors tolerated to MaxAttempts*len(nodes)
const MaxAttempts = 1
const timeout = 2 * time.Second

var verbosity = flag.Int("verbosity", 0, "verbosity level")
var nodes raft.Nodes

func init() {
	flag.Var(&nodes, "node", "server address")
}

type ManagerWithLeader struct {
	sync.Mutex

	*gorums.Manager

	leader *gorums.Node

	nodes   int
	current int

	sequenceNumber uint64
}

func (mgr *ManagerWithLeader) next(leaderHint uint32) {
	if leaderHint != 0 {
		var found bool
		mgr.leader, found = mgr.Node(leaderHint)

		if found {
			return
		}
	}

	mgr.current = (mgr.current + 1) % len(mgr.NodeIDs())
	mgr.leader, _ = mgr.Node(mgr.NodeIDs()[mgr.current])
}

func (mgr *ManagerWithLeader) ClientCommand(command []byte) ([]byte, error) {
	mgr.Lock()
	defer mgr.Unlock()

	mgr.sequenceNumber++

	errs := 0
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		reply, err := mgr.leader.RaftClient.ClientCommand(ctx, &gorums.ClientRequest{Command: command, SequenceNumber: mgr.sequenceNumber})

		if err != nil {
			log.Printf("Error sending ClientCommand to %v: %v", mgr.leader.ID(), err)
			errs++

			if errs >= MaxAttempts*mgr.nodes {
				return nil, ErrClientCommand
			}

			mgr.next(0)

			continue
		}

		switch reply.Status {
		case gorums.OK:
			return reply.Response, nil
		case gorums.SESSION_EXPIRED:
			return nil, ErrSessionExpired
		}

		mgr.next(reply.LeaderHint)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	flag.Parse()

	if len(nodes) == 0 {
		log.Fatal("Missing server addresses.")
	}

	debug.SetVerbosity(*verbosity)

	mgr, err := gorums.NewManager(nodes,
		gorums.WithGrpcDialOptions(
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(time.Second*10)))

	if err != nil {
		log.Fatal("Error creating manager:", err)
	}

	mwl := ManagerWithLeader{
		Manager: mgr,
		leader:  mgr.Nodes(false)[0],
		nodes:   len(mgr.NodeIDs()),
	}

	response, err := mwl.ClientCommand([]byte("SOMETHING"))

	if err != nil {
		log.Fatal(err)

	}

	log.Println(string(response))
}
