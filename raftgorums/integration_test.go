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

func newServer(t *testing.T, wait *sync.WaitGroup, id uint64, n uint64, memory *raft.Memory, electionTimeout time.Duration) (*raftgorums.Raft, *grpc.Server) {
	initialCluster := make([]uint64, n)

	for i := uint64(0); i < n; i++ {
		initialCluster[i] = i + 1
	}

	servers := make([]string, n)

	for i := uint64(0); i < n; i++ {
		servers[i] = fmt.Sprintf(":92%02d", i+1)
	}

	cfg := &raftgorums.Config{
		ID:               id,
		Servers:          servers,
		InitialCluster:   initialCluster,
		Storage:          memory,
		HeartbeatTimeout: 10 * time.Millisecond,
		ElectionTimeout:  electionTimeout,
		Logger: &logrus.Logger{
			Out: ioutil.Discard,
		},
	}

	grpcServer := grpc.NewServer()
	lis, err := net.Listen("tcp", cfg.Servers[id-1])

	if err != nil {
		t.Errorf("could not listen on %s: %v", cfg.Servers[id-1], err)
	}

	go func() {
		grpcServer.Serve(lis)
		wait.Done()
	}()

	return raftgorums.NewRaft(&noopMachine{}, cfg), grpcServer
}

func runServer(t *testing.T, raft *raftgorums.Raft, grpcServer *grpc.Server) {
	err := raft.Run(grpcServer)

	if err != nil {
		t.Error(err)
	}
}

func TestLeaderElection(t *testing.T) {
	logger := &logrus.Logger{
		Out: ioutil.Discard,
	}
	grpclog.SetLogger(logger)

	var n uint64 = 7

	for i := uint64(1); i <= n; i++ {
		t.Run(fmt.Sprintf("leader %d", i), func(t *testing.T) {
			testElectLeader(t, n, i)
		})
	}
}

func testElectLeader(t *testing.T, n uint64, leader uint64) {
	var wg sync.WaitGroup

	timeouts := make([]time.Duration, n)
	kvs := make([]map[uint64]uint64, n)
	logs := make([]map[uint64]*commonpb.Entry, n)
	mems := make([]*raft.Memory, n)
	rafts := make([]*raftgorums.Raft, n)
	grpcServers := make([]*grpc.Server, n)

	for i := uint64(0); i < n; i++ {
		timeouts[i] = time.Second
	}
	timeouts[leader-1] = 50 * time.Millisecond

	for i := uint64(0); i < n; i++ {
		kvs[i], logs[i] = newKVLog()
		mems[i] = raft.NewMemory(kvs[i], logs[i])
		wg.Add(1)
		rafts[i], grpcServers[i] = newServer(t, &wg, i+1, n, mems[i], timeouts[i])
	}

	time.AfterFunc(250*time.Millisecond, func() {
		for i := uint64(0); i < n; i++ {
			rafts[i].Stop()
		}
		for i := uint64(0); i < n; i++ {
			grpcServers[i].GracefulStop()
		}
	})

	for i := uint64(0); i < n; i++ {
		go runServer(t, rafts[i], grpcServers[i])
	}
	wg.Wait()

	checkKVs := func(kvs map[uint64]uint64) uint64 {
		if kvs[raft.KeyTerm] != 1 {
			t.Errorf("term: got %d, want %d", kvs[raft.KeyTerm], 1)
		}
		if kvs[raft.KeyNextIndex] != 2 {
			t.Errorf("next index: got %d, want %d", kvs[raft.KeyNextIndex], 2)
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

	var votes uint64

	for i := uint64(0); i < n; i++ {
		votes += checkKVs(kvs[i])
	}

	if votes < n/2+1 {
		t.Errorf("got %d votes, want at least %d", votes, n/2+1)
	}
}

func newKVLog() (map[uint64]uint64, map[uint64]*commonpb.Entry) {
	return make(map[uint64]uint64), make(map[uint64]*commonpb.Entry)
}
