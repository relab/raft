package main

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/tylertreat/bench"
	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/relab/raft/proto/gorums"
)

// Errors
var (
	ErrRegisterClient = errors.New("Could not register client with leader.")
	ErrClientCommand  = errors.New("Could not send command to leader.")
	ErrSessionExpired = errors.New("Client session has expired.")
	ErrNotLeader      = errors.New("Replica was not leader.")
)

// MaxAttempts sets the maximum number of errors tolerated to MaxAttempts*len(nodes)
const MaxAttempts = 1
const timeout = 10 * time.Second

type ManagerWithLeader struct {
	*gorums.Manager

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

func (mgr *ManagerWithLeader) ClientCommand(command string) error {
	mgr.sequenceNumber++

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	reply, err := mgr.leader.RaftClient.ClientCommand(ctx, &gorums.ClientCommandRequest{Command: command, ClientID: mgr.clientID, SequenceNumber: mgr.sequenceNumber})

	if err != nil {
		mgr.next(0)

		return ErrNotLeader
	}

	switch reply.Status {
	case gorums.OK:
		return nil
	case gorums.SESSION_EXPIRED:
		return ErrSessionExpired
	}

	mgr.next(reply.LeaderHint)

	return ErrNotLeader
}

type ClientRequesterFactory struct {
	Addrs       []string
	PayloadSize int
}

func (r *ClientRequesterFactory) GetRequester(uint64) bench.Requester {
	return &clientRequester{
		addrs:   r.Addrs,
		payload: strings.Repeat("x", r.PayloadSize),
		dialOpts: []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithTimeout(time.Second),
		},
	}
}

type clientRequester struct {
	addrs   []string
	payload string

	dialOpts []grpc.DialOption

	mgr *ManagerWithLeader
}

func (cr *clientRequester) Setup() error {
	mgr, err := gorums.NewManager(cr.addrs,
		gorums.WithGrpcDialOptions(
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(time.Second*10)))

	if err != nil {
		return err
	}

	mwl := &ManagerWithLeader{
		Manager: mgr,
		leader:  mgr.Nodes(false)[0],
		nodes:   len(mgr.NodeIDs()),
	}

	cr.mgr = mwl

	errs := 0
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		reply, err := mwl.leader.RaftClient.ClientCommand(ctx, &gorums.ClientCommandRequest{Command: "REGISTER", SequenceNumber: 0})

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
		case gorums.OK:
			mwl.clientID = reply.ClientID
			return nil
		case gorums.SESSION_EXPIRED:
			return ErrSessionExpired
		}

		mwl.next(reply.LeaderHint)
	}
}

func (cr *clientRequester) Request() error {
	return cr.mgr.ClientCommand(cr.payload)
}

func (cr *clientRequester) Teardown() error {
	cr.mgr.Close()
	cr.mgr = nil
	return nil
}

func main() {
	r := &ClientRequesterFactory{
		Addrs:       []string{":9201", ":9202", ":9203"},
		PayloadSize: 16,
	}

	n := uint64(100)

	benchmark := bench.NewBenchmark(r, 20*n, n, 10*time.Second)
	summary, err := benchmark.Run()

	if err != nil {
		panic(err)
	}

	fmt.Println(summary)
	summary.GenerateLatencyDistribution(bench.Logarithmic, "client.txt")
}
