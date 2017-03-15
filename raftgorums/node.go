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

	grpcServer *grpc.Server

	lookup map[uint64]int
	peers  []string

	match map[uint32]chan uint64

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
		id:         id,
		Raft:       NewRaft(sm, cfg),
		storage:    cfg.Storage,
		grpcServer: server,
		lookup:     lookup,
		peers:      peers,
		match:      make(map[uint32]chan uint64),
		logger:     cfg.Logger,
	}

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

	gorums.RegisterRaftServer(n.grpcServer, n)

	n.mgr = mgr
	n.conf, err = mgr.NewConfiguration(mgr.NodeIDs(), NewQuorumSpec(len(n.peers)+1))

	if err != nil {
		return err
	}

	for _, nodeID := range n.mgr.NodeIDs() {
		n.match[nodeID] = make(chan uint64, 1)
	}

	go n.Raft.Run()

	// January 1, 1970 UTC.
	var lastCuReq time.Time

	for {
		select {
		case req := <-n.Raft.cureqout:
			// TODO Use config.
			if time.Since(lastCuReq) < 100*time.Millisecond {
				continue
			}
			lastCuReq = time.Now()

			n.logger.WithField("matchindex", req.matchIndex).Warnln("Sending catch-up")
			ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
			leader, _ := n.mgr.Node(n.getNodeID(req.leaderID))
			_, err := leader.RaftClient.CatchMeUp(ctx, &pb.CatchMeUpRequest{
				FollowerID: n.id,
				NextIndex:  req.matchIndex + 1,
			})
			cancel()

			if err != nil {
				n.logger.WithError(err).Warnln("CatchMeUp failed")
			}
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
			next := make(map[uint32]uint64)
			nextIndex := req.PrevLogIndex + 1

			for nodeID, ch := range n.match {
				select {
				case index := <-ch:
					// TODO Acessing maxAppendEntries, safe but needs fix.
					atLeastMaxEntries := req.PrevLogIndex+1 > n.Raft.maxAppendEntries
					lessThenMaxEntriesBehind := index < req.PrevLogIndex+1-n.Raft.maxAppendEntries

					if atLeastMaxEntries && lessThenMaxEntriesBehind {
						n.logger.WithField("gorumsid", nodeID).Warnln("Server too far behind")
						index = req.PrevLogIndex + 1
					}
					next[nodeID] = index
					if index < nextIndex {
						nextIndex = index
					}
				default:
				}
			}

			// TODO This should be safe as it only accesses storage
			// which uses transactions. TODO It accesses
			// maxAppendEntries but this on does not change after
			// startup.
			entries := n.Raft.getNextEntries(nextIndex)
			e := uint64(len(entries))
			maxIndex := nextIndex + e - 1

			ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
			res, err := n.conf.AppendEntries(ctx, req,
				// These functions will be executed concurrently.
				func(req pb.AppendEntriesRequest, nodeID uint32) *pb.AppendEntriesRequest {
					if index, ok := next[nodeID]; ok {
						req.PrevLogIndex = index - 1
						// TODO This should be safe as
						// it only accesses storage
						// which uses transactions.
						req.PrevLogTerm = n.Raft.logTerm(index - 1)
					}

					need := maxIndex - req.PrevLogIndex
					req.Entries = entries[e-need:]

					n.logger.WithFields(logrus.Fields{
						"prevlogindex": req.PrevLogIndex,
						"prevlogterm":  req.PrevLogTerm,
						"commitindex":  req.CommitIndex,
						"currentterm":  req.Term,
						"lenentries":   len(req.Entries),
						"gorumsid":     nodeID,
					}).Infoln("Sending AppendEntries")

					return &req
				},
			)

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
	n.match[n.getNodeID(req.FollowerID)] <- req.NextIndex
	return
}

func (n *Node) getNodeID(raftID uint64) uint32 {
	return n.mgr.NodeIDs()[n.lookup[raftID-1]]
}
