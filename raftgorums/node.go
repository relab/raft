package raftgorums

import (
	"io/ioutil"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/Sirupsen/logrus"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
	gorums "github.com/relab/raft/raftgorums/gorumspb"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

// Node ties an instance of Raft to the network.
type Node struct {
	id uint64

	Raft    *Raft
	storage Storage

	lookup map[uint64]int
	peers  []string

	mgr  *gorums.Manager
	conf *gorums.Configuration

	logger logrus.FieldLogger
}

// NewNode returns a Node with an instance of Raft given the configuration.
func NewNode(server *grpc.Server, sm raft.StateMachine, cfg *Config) *Node {
	peers := make([]string, len(cfg.Nodes))
	// We don't want to mutate cfg.Nodes.
	copy(peers, cfg.Nodes)

	id := cfg.ID
	// Exclude self.
	peers = append(peers[:id-1], peers[id:]...)

	var pos int
	lookup := make(map[uint64]int)

	for i := 0; i < len(cfg.Nodes); i++ {
		if uint64(i)+1 == id {
			continue
		}

		lookup[uint64(i)] = pos
		pos++
	}

	if cfg.Logger == nil {
		l := logrus.New()
		l.Out = ioutil.Discard
		cfg.Logger = l
	}

	cfg.Logger = cfg.Logger.WithField("nodeid", cfg.ID)

	n := &Node{
		id:      id,
		Raft:    NewRaft(sm, cfg),
		storage: cfg.Storage,
		lookup:  lookup,
		peers:   peers,
		logger:  cfg.Logger,
	}

	gorums.RegisterRaftServer(server, n)

	return n
}

// Run start listening for incoming messages, and delivers outgoing messages.
func (n *Node) Run() error {
	opts := []gorums.ManagerOption{
		gorums.WithGrpcDialOptions(
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(TCPConnect*time.Millisecond)),
	}

	mgr, err := gorums.NewManager(n.peers, opts...)

	if err != nil {
		return err
	}

	n.mgr = mgr
	n.conf, err = mgr.NewConfiguration(mgr.NodeIDs(), NewQuorumSpec(len(n.peers)+1))

	if err != nil {
		return err
	}

	go n.Raft.Run()

	for {
		select {
		case req := <-n.Raft.rvreqout:
			ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
			res, err := n.conf.RequestVote(ctx, req)
			cancel()

			if err != nil {
				n.logger.WithError(err).Warnln("RequestVote failed")
			}

			if res.RequestVoteResponse == nil {
				continue
			}

			n.Raft.HandleRequestVoteResponse(res.RequestVoteResponse)

		case req := <-n.Raft.aereqout:
			ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
			res, err := n.conf.AppendEntries(ctx, req)

			if err != nil {
				n.logger.WithError(err).Warnln("AppendEntries failed")
			}

			if res.AppendEntriesResponse == nil {
				continue
			}

			// Cancel on abort.
			if !res.AppendEntriesResponse.Success {
				cancel()
			}

			n.Raft.HandleAppendEntriesResponse(res.AppendEntriesResponse, len(res.NodeIDs))
		}
	}
}

// RequestVote implements gorums.RaftServer.
func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return n.Raft.HandleRequestVoteRequest(req), nil
}

// AppendEntries implements gorums.RaftServer.
func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return n.Raft.HandleAppendEntriesRequest(req), nil
}

// InstallSnapshot implements gorums.RaftServer.
func (n *Node) InstallSnapshot(ctx context.Context, snapshot *commonpb.Snapshot) (*pb.InstallSnapshotResponse, error) {
	return n.Raft.HandleInstallSnapshotRequest(snapshot), nil
}

// CatchMeUp implements gorums.RaftServer.
func (n *Node) CatchMeUp(ctx context.Context, req *pb.CatchMeUpRequest) (res *pb.Empty, err error) {
	res = &pb.Empty{}
	return
}

func (n *Node) getNodeID(raftID uint64) uint32 {
	return n.mgr.NodeIDs()[n.lookup[raftID-1]]
}
