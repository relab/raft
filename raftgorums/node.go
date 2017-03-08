package raftgorums

import (
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

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

	cLock      sync.Mutex
	catchingUp map[uint32]chan uint64

	mgr  *gorums.Manager
	conf *gorums.Configuration
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

	n := &Node{
		id:         id,
		Raft:       NewRaft(sm, cfg),
		storage:    cfg.Storage,
		lookup:     lookup,
		peers:      peers,
		catchingUp: make(map[uint32]chan uint64),
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
		rvreqout := n.Raft.RequestVoteRequestChan()
		aereqout := n.Raft.AppendEntriesRequestChan()
		sreqout := n.Raft.snapshotRequestChan()
		cureqout := n.Raft.catchUpRequestChan()

		select {
		case req := <-cureqout:
			leaderID := n.getNodeID(req.leaderID)
			// We can ignore the found return value as we are
			// getting the ID directly from the manager, i.e., it is
			// never missing.
			leader, _ := n.mgr.Node(leaderID)

			// Don't block normal operation.
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
				// We ignore the error here as a failure to catch-up
				// will eventually trigger a new catch-up request.
				leader.RaftClient.CatchMeUp(ctx, &pb.CatchMeUpRequest{
					FollowerID: n.id,
				})
				cancel()
			}()

		case req := <-sreqout:
			followerID := n.getNodeID(req.followerID)
			n.cLock.Lock()
			matchCh := n.catchingUp[followerID]
			n.cLock.Unlock()

			// Continue if we can't remove the node from the
			// configuration due to the quorum size becoming to
			// small.
			if ok := n.removeNode(followerID); !ok {
				n.cLock.Lock()
				delete(n.catchingUp, followerID)
				n.cLock.Unlock()
				continue
			}

			// We can ignore the found return value as we are
			// getting the ID directly from the manager, i.e., it is
			// never missing.
			follower, _ := n.mgr.Node(followerID)

			// Transferring the snapshot might take substantial
			// time, do it asynchronously.
			go func() {
				defer func() {
					n.cLock.Lock()
					_, ok := n.catchingUp[followerID]
					delete(n.catchingUp, followerID)
					n.cLock.Unlock()

					if ok {
						n.addNode(followerID)
					}
				}()

				ctx, cancel := context.WithTimeout(context.Background(), TCPConnect*time.Millisecond)
				res, err := follower.RaftClient.InstallSnapshot(ctx, req.snapshot)
				cancel()

				if err != nil {
					// TODO Better error message.
					log.Println(fmt.Sprintf("InstallSnapshot failed = %v", err))
					return
				}

				// If follower is in a higher term, return.
				if !n.Raft.HandleInstallSnapshotResponse(res) {
					return
				}

				// We use 2 peers as we need to count an implicit leader.
				single, err := n.mgr.NewConfiguration([]uint32{followerID}, NewQuorumSpec(2))

				if err != nil {
					panic(fmt.Sprintf("tried to create new configuration %v: %v",
						[]uint32{followerID}, err,
					))
				}

				n.Raft.catchUp(single, req.snapshot.LastIncludedIndex+1, matchCh)
			}()

		case req := <-rvreqout:
			ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
			res, err := n.conf.RequestVote(ctx, req)
			cancel()

			if err != nil {
				// TODO Better error message.
				log.Println(fmt.Sprintf("RequestVote failed = %v", err))

			}

			if res.RequestVoteResponse == nil {
				continue
			}

			n.Raft.HandleRequestVoteResponse(res.RequestVoteResponse)

		case req := <-aereqout:
			ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
			res, err := n.conf.AppendEntries(ctx, req)

			if err != nil {
				// TODO Better error message.
				log.Println(fmt.Sprintf("AppendEntries failed = %v", err))

				if res.AppendEntriesResponse == nil {
					continue
				}
			}

			// Cancel on abort.
			if !res.AppendEntriesResponse.Success {
				cancel()
			}

			n.Raft.HandleAppendEntriesResponse(res.AppendEntriesResponse, len(res.NodeIDs))

			n.cLock.Lock()
			for nodeID, matchIndex := range n.catchingUp {
				select {
				case index, ok := <-matchIndex:
					if !ok {
						delete(n.catchingUp, nodeID)
						n.addNode(nodeID)
						continue
					}

					if index == res.MatchIndex {
						delete(n.catchingUp, nodeID)
						n.addNode(nodeID)
					}

					matchIndex <- res.MatchIndex
				default:
				}
			}
			n.cLock.Unlock()
		}
	}
}

// removeNode must be called from the select in node.Run().
func (n *Node) removeNode(nodeID uint32) bool {
	oldSet := n.conf.NodeIDs()

	// Don't remove servers when we would've gone below the
	// quorum size. The quorum function handles recovery
	// when a majority fails.
	if len(oldSet)-1 < len(n.peers)/2 {
		return false
	}

	tmpSet := make([]uint32, len(oldSet)-1)

	// Exclude node from main configuration.
	var i int
	for _, id := range oldSet {
		if id == nodeID {
			continue
		}

		tmpSet[i] = id
		i++
	}

	// It's important not to change the quorum size when
	// removing the server. We reduce N by one so we don't
	// wait on the recovering server though.
	var err error
	n.conf, err = n.mgr.NewConfiguration(tmpSet, &QuorumSpec{
		N: len(tmpSet),
		Q: (len(n.peers) + 1) / 2,
	})

	if err != nil {
		panic(fmt.Sprintf("tried to create new configuration %v: %v", tmpSet, err))
	}

	return true
}

// addNode must be called from the select in node.Run().
func (n *Node) addNode(nodeID uint32) {
	newSet := append(n.conf.NodeIDs(), nodeID)
	var err error
	n.conf, err = n.mgr.NewConfiguration(newSet, &QuorumSpec{
		N: len(newSet),
		Q: (len(n.peers) + 1) / 2,
	})

	if err != nil {
		panic(fmt.Sprintf("tried to create new configuration %v: %v", newSet, err))
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

	followerID := n.getNodeID(req.FollowerID)

	matchCh := make(chan uint64)
	n.cLock.Lock()
	_, ok := n.catchingUp[followerID]
	if !ok {
		n.catchingUp[followerID] = matchCh
	}
	n.cLock.Unlock()

	// Don't start another catch-up routine if we already have one.
	if ok {
		return
	}

	n.Raft.HandleCatchMeUpRequest(req)
	return
}

func (n *Node) getNodeID(raftID uint64) uint32 {
	return n.mgr.NodeIDs()[n.lookup[raftID-1]]
}
