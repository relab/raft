package raftgorums_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"github.com/Sirupsen/logrus"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
	"github.com/relab/raft/raftgorums"
)

// These tests depend heavily on the speed of your computer. You might need to
// increase some of these timeouts to make the tests pass.
var (
	heartbeat  = 20 * time.Millisecond
	election   = 25 * time.Millisecond
	longEnough = 150 * time.Millisecond
	wait       = 100 * time.Millisecond
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
			if i != j {
				t.Run(fmt.Sprintf("leader %d, n: %d, add %d", j, i, i), func(t *testing.T) {
					testProposeConfAdd(t, i, j)
				})
			}
			t.Run(fmt.Sprintf("leader %d, n: %d, remove %d", j, i, i), func(t *testing.T) {
				testProposeConfRemove(t, i, j)
			})
			t.Run(fmt.Sprintf("leader propose cmd %d, n: %d", j, i), func(t *testing.T) {
				testProposeCmdRead(t, i, j)
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

func newTestServer(t *testing.T, wg *sync.WaitGroup, c *cfg, port uint64, exclude ...uint64) *testServer {
	initialCluster := make([]uint64, c.n)
	if len(exclude) > 0 {
		initialCluster = make([]uint64, c.n-1)
	}

	for i := uint64(0); i < uint64(len(initialCluster)); i++ {
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
		HeartbeatTimeout: heartbeat,
		ElectionTimeout:  c.electionTimeout,
		Logger: &logrus.Logger{
			Out: ioutil.Discard,
		},
	}

	grpcServer := grpc.NewServer()
	lis, err := net.Listen("tcp", cfg.Servers[c.id-1])

	if err != nil {
		t.Errorf("could not listen on %s: %v", cfg.Servers[c.id-1], err)
		t.FailNow()
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
		t.t.FailNow()
	}
}

func testElectLeader(t *testing.T, n uint64, leader uint64) {
	var wg sync.WaitGroup

	servers := make(map[uint64]*testServer, n)

	p := port
	port += n + 10

	for i := n; i > 0; i-- {
		wg.Add(1)
		timeout := longEnough
		if i == leader {
			timeout = election
		}
		servers[i] = newTestServer(t, &wg, &cfg{
			id:              i,
			n:               n,
			electionTimeout: timeout,
		}, p)
	}

	time.AfterFunc(longEnough, func() {
		for i := n; i > 0; i-- {
			servers[i].Stop()
		}
	})

	for i := n; i > 0; i-- {
		go servers[i].Run()
	}
	wg.Wait()

	if servers[leader].kv[raft.KeyTerm] == 0 {
		t.Skipf("skipping test as no leader was elected")
	}

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
	port += n + 10

	for i := n; i > 0; i-- {
		wg.Add(1)
		timeout := longEnough
		if i == leader {
			timeout = election
		}
		servers[i] = newTestServer(t, &wg, &cfg{
			id:              i,
			n:               n,
			electionTimeout: timeout,
		}, p)
	}

	time.AfterFunc(longEnough, func() {
		for i := n; i > 0; i-- {
			if i != leader {
				servers[i].Stop()
			}
		}

		time.Sleep(3 * servers[leader].timeout)
		servers[leader].Stop()
	})

	for i := n; i > 0; i-- {
		go servers[i].Run()
	}
	wg.Wait()

	if servers[leader].kv[raft.KeyTerm] == 0 {
		t.Skipf("skipping test as no leader was elected")
	}

	var votes uint64

	for i := n; i > 0; i-- {
		votes += checkKVs(t, servers[i].kv, 1, 2, leader)
	}

	checkLeaderState(t, servers[leader].raft.State(), raftgorums.Candidate)
	checkFollowersState(t, servers, leader)
	checkVotes(t, votes, n)
}

func testProposeConfAdd(t *testing.T, n uint64, leader uint64) {
	var wg sync.WaitGroup

	servers := make(map[uint64]*testServer, n)

	p := port
	port += n + 10

	for i := n; i > 0; i-- {
		wg.Add(1)
		timeout := longEnough
		if i == leader {
			timeout = election
		}
		servers[i] = newTestServer(t, &wg, &cfg{
			id:              i,
			n:               n,
			electionTimeout: timeout,
		}, p, n)
	}

	time.AfterFunc(longEnough, func() {
		for i := n; i > 0; i-- {
			servers[i].Stop()
		}
	})

	for i := n; i > 0; i-- {
		go servers[i].Run()
	}

	time.Sleep(wait)

	if servers[leader].kv[raft.KeyTerm] == 0 {
		t.Skipf("skipping test as no leader was elected")
	}

	future, err := servers[leader].raft.ProposeConf(context.Background(), &commonpb.ReconfRequest{
		ServerID:   n,
		ReconfType: commonpb.ReconfAdd,
	})

	if err != nil {
		t.Error(err)
		return
	}

	for i := n; i > 0; i-- {
		if i == leader {
			continue
		}

		_, err := servers[i].raft.ProposeConf(context.Background(), &commonpb.ReconfRequest{})

		if err == nil {
			t.Errorf("can't propose conf on non-leader %d", i)
			return
		}
	}

	res := <-future.ResultCh()
	reconfStatus := res.Value.(*commonpb.ReconfResponse).Status

	if reconfStatus != commonpb.ReconfOK {
		t.Errorf("reconf: got %d, want %d", reconfStatus, commonpb.ReconfOK)
		return
	}

	wg.Wait()

	var votes uint64

	for i := n; i > 0; i-- {
		votes += checkKVs(t, servers[i].kv, 1, 3, leader)
	}

	checkLeaderState(t, servers[leader].raft.State(), raftgorums.Leader)
	checkFollowersState(t, servers, leader)
	checkVotes(t, votes, n-1)
}

func testProposeConfRemove(t *testing.T, n uint64, leader uint64) {
	var wg sync.WaitGroup

	servers := make(map[uint64]*testServer, n)

	p := port
	port += n + 10

	for i := n; i > 0; i-- {
		wg.Add(1)
		timeout := longEnough
		if i == leader {
			timeout = election
		}
		servers[i] = newTestServer(t, &wg, &cfg{
			id:              i,
			n:               n,
			electionTimeout: timeout,
		}, p)
	}

	time.AfterFunc(longEnough, func() {
		for i := n; i > 0; i-- {
			servers[i].Stop()
		}
	})

	for i := n; i > 0; i-- {
		go servers[i].Run()
	}

	time.Sleep(wait)

	if servers[leader].kv[raft.KeyTerm] == 0 {
		t.Skipf("skipping test as no leader was elected")
	}

	future, err := servers[leader].raft.ProposeConf(context.Background(), &commonpb.ReconfRequest{
		ServerID:   n,
		ReconfType: commonpb.ReconfRemove,
	})

	if err != nil {
		t.Error(err)
		return
	}

	res := <-future.ResultCh()
	reconfStatus := res.Value.(*commonpb.ReconfResponse).Status

	if n > 2 {
		if reconfStatus != commonpb.ReconfOK {
			t.Errorf("reconf: got %d, want %d", reconfStatus, commonpb.ReconfOK)
			return
		}
	} else {
		// Cannot do reconf. if next config size < 2.
		if reconfStatus != commonpb.ReconfTimeout {
			t.Errorf("reconf: got %d, want %d", reconfStatus, commonpb.ReconfTimeout)
			return
		}
	}

	wg.Wait()

	var votes uint64

	if leader == n && n != 2 {
		checkLeaderState(t, servers[leader].raft.State(), raftgorums.Inactive)
		votes = checkKVs(t, servers[n].kv, 1, 3, leader)
	} else {
		checkLeaderState(t, servers[leader].raft.State(), raftgorums.Leader)
		votes = checkKVs(t, servers[n].kv, 1, 2, leader)
	}

	if n == 2 {
		votes += checkKVs(t, servers[1].kv, 1, 2, leader)
	} else {
		for i := n - 1; i > 0; i-- {
			votes += checkKVs(t, servers[i].kv, 1, 3, leader)
		}
	}

	checkFollowersState(t, servers, leader)
	checkVotes(t, votes, n)
}

func testProposeCmdRead(t *testing.T, n uint64, leader uint64) {
	var wg sync.WaitGroup

	servers := make(map[uint64]*testServer, n)

	p := port
	port += n + 10

	for i := n; i > 0; i-- {
		wg.Add(1)
		timeout := longEnough
		if i == leader {
			timeout = election
		}
		servers[i] = newTestServer(t, &wg, &cfg{
			id:              i,
			n:               n,
			electionTimeout: timeout,
		}, p)
	}

	time.AfterFunc(longEnough, func() {
		for i := n; i > 0; i-- {
			servers[i].Stop()
		}
	})

	for i := n; i > 0; i-- {
		go servers[i].Run()
	}

	time.Sleep(wait)

	if servers[leader].kv[raft.KeyTerm] == 0 {
		t.Skipf("skipping test as no leader was elected")
	}

	future, err := servers[leader].raft.ProposeCmd(context.Background(), raft.NOOP)

	if err != nil {
		t.Error(err)
		return
	}

	for i := n; i > 0; i-- {
		if i == leader {
			continue
		}

		_, err := servers[i].raft.ProposeCmd(context.Background(), raft.NOOP)

		if err == nil {
			t.Errorf("can't propose cmd on non-leader %d", i)
			return
		}
	}

	res := <-future.ResultCh()

	entry := res.Value.(*commonpb.Entry)

	if res.Index != 2 {
		t.Errorf("index: got %d, want %d", res.Index, 2)
	}

	if !bytes.Equal(entry.Data, raft.NOOP) {
		t.Errorf("data: got %s, want %s", entry.Data, raft.NOOP)
	}

	future, err = servers[leader].raft.ReadCmd(context.Background(), raft.NOOP)

	if err != nil {
		t.Error(err)
		return
	}

	for i := n; i > 0; i-- {
		if i == leader {
			continue
		}

		_, err := servers[i].raft.ReadCmd(context.Background(), raft.NOOP)

		if err == nil {
			t.Errorf("can't read cmd on non-leader %d", i)
			return
		}
	}

	res = <-future.ResultCh()

	entry = res.Value.(*commonpb.Entry)

	if res.Index != 0 {
		t.Errorf("index: got %d, want %d", res.Index, 0)
	}

	if !bytes.Equal(entry.Data, raft.NOOP) {
		t.Errorf("data: got %s, want %s", entry.Data, raft.NOOP)
	}

	wg.Wait()

	var votes uint64

	for i := n; i > 0; i-- {
		votes += checkKVs(t, servers[i].kv, 1, 3, leader)
	}

	checkLeaderState(t, servers[leader].raft.State(), raftgorums.Leader)
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
