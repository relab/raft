package raftgorums_test

import (
	"fmt"
	"io/ioutil"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"github.com/Sirupsen/logrus"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
	"github.com/relab/raft/raftgorums"
)

func TestLeaderElection(t *testing.T) {
	logger := &logrus.Logger{
		Out: ioutil.Discard,
	}
	grpclog.SetLogger(logger)

	var n uint64 = 7

	for i := n; i > 1; i-- {
		for j := i; j > 1; j-- {
			t.Run(fmt.Sprintf("leader %d, n: %d", j, i), func(t *testing.T) {
				testElectLeader(t, i, j)
			})
			t.Run(fmt.Sprintf("leader stepdown %d, n: %d", j, i), func(t *testing.T) {
				testElectLeaderStepDown(t, i, j)
			})
		}
	}
}

type cfg struct {
	id              uint64
	n               uint64
	electionTimeout time.Duration
}

type testServer struct {
	t  *testing.T
	wg *sync.WaitGroup

	timeout    time.Duration
	kv         map[uint64]uint64
	log        map[uint64]*commonpb.Entry
	mem        *raft.Memory
	raft       *raftgorums.Raft
	grpcServer *grpc.Server
}

var port uint64 = 9201

func newTestServer(t *testing.T, wg *sync.WaitGroup, c *cfg, port uint64) *testServer {
	initialCluster := make([]uint64, c.n)

	for i := uint64(0); i < c.n; i++ {
		initialCluster[i] = i + 1
	}

	servers := make([]string, c.n)

	for i := c.n; i > 0; i-- {
		servers[i-1] = fmt.Sprintf(":%d", port)
		port++
	}

	kv := make(map[uint64]uint64)
	raftLog := make(map[uint64]*commonpb.Entry)

	server := &testServer{
		t:       t,
		wg:      wg,
		timeout: c.electionTimeout,
		kv:      kv,
		log:     raftLog,
		mem:     raft.NewMemory(kv, raftLog),
	}

	cfg := &raftgorums.Config{
		ID:               c.id,
		Servers:          servers,
		InitialCluster:   initialCluster,
		Storage:          server.mem,
		HeartbeatTimeout: 10 * time.Millisecond,
		ElectionTimeout:  c.electionTimeout,
		Logger: &logrus.Logger{
			Out: ioutil.Discard,
		},
	}

	grpcServer := grpc.NewServer()
	lis, err := net.Listen("tcp", cfg.Servers[c.id-1])

	if err != nil {
		t.Errorf("could not listen on %s: %v", cfg.Servers[c.id-1], err)
	}

	go func() {
		grpcServer.Serve(lis)
		wg.Done()
	}()

	raft := raftgorums.NewRaft(&noopMachine{}, cfg)

	server.grpcServer = grpcServer
	server.raft = raft

	return server
}

func (t *testServer) Stop() {
	t.raft.Stop()
	t.grpcServer.Stop()
}

func (t *testServer) Run() {
	err := t.raft.Run(t.grpcServer)

	if err != nil {
		t.t.Error(err)
	}
}

func testElectLeader(t *testing.T, n uint64, leader uint64) {
	var wg sync.WaitGroup

	servers := make(map[uint64]*testServer, n)

	p := port
	port += n + 1

	for i := n; i > 0; i-- {
		wg.Add(1)
		timeout := time.Second
		if i == leader {
			timeout = 25 * time.Millisecond
		}
		servers[i] = newTestServer(t, &wg, &cfg{
			id:              i,
			n:               n,
			electionTimeout: timeout,
		}, p)
	}

	time.AfterFunc(500*time.Millisecond, func() {
		for i := n; i > 0; i-- {
			servers[i].Stop()
		}
	})

	for i := n; i > 0; i-- {
		go servers[i].Run()
	}
	wg.Wait()

	var votes uint64

	for i := n; i > 0; i-- {
		votes += checkKVs(t, servers[i].kv, 1, 2, leader)
	}

	checkLeaderState(t, servers[leader].raft.State(), raftgorums.Leader)
	checkFollowersState(t, servers, leader)
	checkVotes(t, votes, n)
}

func testElectLeaderStepDown(t *testing.T, n uint64, leader uint64) {
	var wg sync.WaitGroup

	servers := make(map[uint64]*testServer, n)

	p := port
	port += n + 1

	for i := n; i > 0; i-- {
		wg.Add(1)
		timeout := time.Second
		if i == leader {
			timeout = 25 * time.Millisecond
		}
		servers[i] = newTestServer(t, &wg, &cfg{
			id:              i,
			n:               n,
			electionTimeout: timeout,
		}, p)
	}

	time.AfterFunc(500*time.Millisecond, func() {
		for i := n; i > 0; i-- {
			if i != leader {
				servers[i].Stop()
			}
		}

		time.Sleep(4 * servers[leader].timeout)
		servers[leader].Stop()
	})

	for i := n; i > 0; i-- {
		go servers[i].Run()
	}
	wg.Wait()

	var votes uint64

	for i := n; i > 0; i-- {
		votes += checkKVs(t, servers[i].kv, 1, 2, leader)
	}

	checkLeaderState(t, servers[leader].raft.State(), raftgorums.Candidate)
	checkFollowersState(t, servers, leader)
	checkVotes(t, votes, n)
}

func checkFollowersState(t *testing.T, servers map[uint64]*testServer, leader uint64) {
	for id, server := range servers {
		if id == leader {
			continue
		}
		if server.raft.State() != raftgorums.Follower {
			t.Errorf("unexpected follower state: got %v, want %v", server.raft.State(), raftgorums.Follower)
		}
	}
}

func checkLeaderState(t *testing.T, got, want raftgorums.State) {
	if want == raftgorums.Candidate {
		if got > want {
			t.Errorf("unexpected leader state: got %v, want at most %v", got, want)
		}
		return
	}
	if got != want {
		t.Errorf("unexpected leader state: got %v, want %v", got, want)
	}
}

func checkVotes(t *testing.T, votes, n uint64) {
	if votes < n/2+1 {
		t.Errorf("got %d votes, want at least %d", votes, n/2+1)
	}
}

func checkKVs(t *testing.T, kvs map[uint64]uint64, term, nextIndex, leader uint64) uint64 {
	if kvs[raft.KeyTerm] != term {
		t.Errorf("term: got %d, want %d", kvs[raft.KeyTerm], term)
	}
	if kvs[raft.KeyNextIndex] != nextIndex {
		t.Errorf("next index: got %d, want %d", kvs[raft.KeyNextIndex], nextIndex)
	}
	votedFor := kvs[raft.KeyVotedFor]
	if votedFor != leader && votedFor != raftgorums.None {
		t.Errorf("voted for: got %d, want %d", kvs[raft.KeyVotedFor], leader)
	}

	if votedFor == leader {
		return 1
	}
	return 0
}
