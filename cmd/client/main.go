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
	ErrRegisterClient = errors.New("Could not register client with leader")
	ErrClientCommand  = errors.New("Could not send command to leader")
	ErrSessionExpired = errors.New("Client session has expired")
)

// MaxAttempts sets the maximum number of errors tolerated to MaxAttempts*len(nodes)
const MaxAttempts = 1
const timeout = 2 * time.Second

type ManagerWithLeader struct {
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

type ClientRequesterFactory struct {
	Addrs       []string
	PayloadSize int
}

func (r *ClientRequesterFactory) GetRequester(uint64) bench.Requester {
	return &clientRequester{
		addrs:   r.Addrs,
		payload: []byte(strings.Repeat("x", r.PayloadSize)),
		dialOpts: []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithTimeout(time.Second),
		},
	}
}

type clientRequester struct {
	addrs   []string
	payload []byte

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
		return nil
	}

	mwl := &ManagerWithLeader{
		Manager: mgr,
		leader:  mgr.Nodes(false)[0],
		nodes:   len(mgr.NodeIDs()),
	}

	cr.mgr = mwl

	return nil
}

func (cr *clientRequester) Request() error {
	_, err := cr.mgr.ClientCommand(cr.payload)

	return err
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

	benchmark := bench.NewBenchmark(r, 0, 1, 30*time.Second)
	summary, err := benchmark.Run()

	if err != nil {
		panic(err)
	}

	fmt.Println(summary)
	summary.GenerateLatencyDistribution(bench.Logarithmic, "client.txt")
}
