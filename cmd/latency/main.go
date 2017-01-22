package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/tylertreat/bench"
	"golang.org/x/net/context"

	"google.golang.org/grpc"

	gorums "github.com/relab/raft/proto/gorums"
	pb "github.com/relab/raft/proto/messages"
)

// Errors
var (
	ErrClientCommand  = errors.New("Could not send command to leader.")
	ErrSessionExpired = errors.New("Client session has expired.")
	ErrNotLeader      = errors.New("Replica was not leader.")
)

// MaxAttempts sets the maximum number of errors tolerated to MaxAttempts*len(nodes)
const MaxAttempts = 1

const t = 10 * time.Second

var output = flag.String("output", "client.txt", "What to use as the base for the output filenames")
var clients = flag.Int("clients", 15, "Number of clients")
var rate = flag.Int("rate", 15, "How many requests each client sends per second")
var timeout = flag.Duration("time", time.Second*30, "How long to measure in `seconds`")
var nodes Nodes

// ManagerWithLeader is a *gorums.Manager containing information about which replica is currently the leader.
type ManagerWithLeader struct {
	*gorums.Manager

	this   sync.Mutex
	leader *gorums.Node

	nodes   int
	current int

	clientID       uint32
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

// ClientCommand invokes the ClientCommand RPC on the cluster's leader.
func (mgr *ManagerWithLeader) ClientCommand(command string) error {
	mgr.this.Lock()
	defer mgr.this.Unlock()

	mgr.sequenceNumber++

	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()

	reply, err := mgr.leader.RaftClient.ClientCommand(ctx, &pb.ClientCommandRequest{Command: command, ClientID: mgr.clientID, SequenceNumber: mgr.sequenceNumber})

	if err != nil {
		mgr.next(0)

		return err
	}

	switch reply.Status {
	case pb.OK:
		return nil
	case pb.SESSION_EXPIRED:
		return ErrSessionExpired
	}

	log.Println(reply.LeaderHint)

	mgr.next(reply.LeaderHint)

	return ErrNotLeader
}

// ClientRequesterFactory is used to create multiple unique clients.
type ClientRequesterFactory struct {
	Addrs       []string
	PayloadSize int

	done chan int
}

// GetRequester gets a new client.
func (r *ClientRequesterFactory) GetRequester(uint64) bench.Requester {
	return &clientRequester{
		addrs:   r.Addrs,
		payload: strings.Repeat("x", r.PayloadSize),
		dialOpts: []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithTimeout(time.Second),
		},
		done: r.done,
	}
}

type clientRequester struct {
	addrs   []string
	payload string

	dialOpts []grpc.DialOption

	mgr *ManagerWithLeader

	done chan int
}

func (cr *clientRequester) Setup() error {
	mgr, err := gorums.NewManager(cr.addrs,
		gorums.WithGrpcDialOptions(cr.dialOpts...))

	if err != nil {
		return err
	}

	mwl := &ManagerWithLeader{
		Manager: mgr,
		leader:  mgr.Nodes()[0],
		nodes:   len(mgr.NodeIDs()),
	}

	cr.mgr = mwl

	errs := 0
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()

	for {
		reply, err := mwl.leader.RaftClient.ClientCommand(ctx, &pb.ClientCommandRequest{Command: "REGISTER", SequenceNumber: 0})

		if err != nil {
			log.Printf("Error sending ClientCommand to %v: %v", mwl.leader.ID(), err)
			errs++

			if errs >= MaxAttempts*mwl.nodes {
				return ErrClientCommand
			}

			mwl.next(0)

			continue
		}

		switch reply.Status {
		case pb.OK:
			mwl.clientID = reply.ClientID
			return nil
		case pb.SESSION_EXPIRED:
			return ErrSessionExpired
		}

		mwl.next(reply.LeaderHint)
	}
}

func (cr *clientRequester) Request() error {
	return cr.mgr.ClientCommand(cr.payload)
}

func (cr *clientRequester) Teardown() error {
	select {
	case <-cr.done:
	default:
		close(cr.done)
	}

	cr.mgr.Close()
	cr.mgr = nil
	return nil
}

func main() {
	flag.Var(&nodes, "add", "Remote server address, repeat argument for each server in the cluster")
	flag.Parse()

	if len(nodes) == 0 {
		fmt.Print("-add argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	done := make(chan int)

	r := &ClientRequesterFactory{
		Addrs:       nodes,
		PayloadSize: 16,
		done:        done,
	}

	benchmark := bench.NewBenchmark(r, uint64(*rate)*uint64(*clients), uint64(*clients), *timeout)
	summary, err := benchmark.Run()

	if err != nil {
		panic(err)
	}

	fmt.Println(summary)
	summary.GenerateLatencyDistribution(bench.Logarithmic, *output)
}
