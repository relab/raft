package raftgorums

import (
	"fmt"
	"log"
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

	catchingUp map[uint32]chan uint64
	catchUp    chan *catchUpRequest

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
		if uint64(i) == id {
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
		catchUp:    make(chan *catchUpRequest),
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
		sreqout := n.Raft.SnapshotRequestChan()

		select {
		case req := <-sreqout:
			ctx, cancel := context.WithTimeout(context.Background(), TCPConnect*time.Millisecond)
			node, _ := n.mgr.Node(n.getNodeID(req.FollowerID))
			snapshot, err := node.RaftClient.GetState(ctx, req)
			cancel()

			if err != nil {
				// TODO Better error message.
				log.Println(fmt.Sprintf("Snapshot request failed = %v", err))

			}

			n.Raft.restoreCh <- snapshot

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

			for nodeID, matchIndex := range n.catchingUp {
				select {
				case index, ok := <-matchIndex:
					if !ok {
						n.addBackNode(nodeID)
						continue
					}

					matchIndex <- res.MatchIndex

					if index == res.MatchIndex {
						n.addBackNode(nodeID)
					}
				default:
				}
			}

		case creq := <-n.catchUp:
			oldSet := n.conf.NodeIDs()

			// Don't remove servers when we would've gone below the
			// quorum size. The quorum function handles recovery
			// when a majority fails.
			if len(oldSet)-1 < len(n.peers)/2 {
				continue
			}

			node := n.getNodeID(creq.followerID)
			// We use 2 peers as we need to count the leader.
			single, err := n.mgr.NewConfiguration([]uint32{node}, NewQuorumSpec(2))

			if err != nil {
				panic(fmt.Sprintf("tried to catch up node %d->%d: %v", creq.followerID, node, err))
			}

			tmpSet := make([]uint32, len(oldSet)-1)

			var i int
			for _, id := range oldSet {
				if id == node {
					continue
				}

				tmpSet[i] = id
				i++
			}

			// It's important not to change the quorum size when
			// removing the server. We reduce N by one so we don't
			// wait on the recovering server.
			n.conf, err = mgr.NewConfiguration(tmpSet, &QuorumSpec{
				N: len(tmpSet),
				Q: (len(n.peers) + 1) / 2,
			})

			if err != nil {
				panic(fmt.Sprintf("tried to create new configuration %v: %v", tmpSet, err))
			}

			matchIndex := make(chan uint64)
			n.catchingUp[node] = matchIndex
			go n.doCatchUp(single, creq.nextIndex, matchIndex)
		}
	}
}

func (n *Node) addBackNode(nodeID uint32) {
	n.Raft.Lock()
	n.Raft.catchingUp = false
	n.Raft.Unlock()
	delete(n.catchingUp, nodeID)

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

func (n *Node) GetState(ctx context.Context, req *pb.SnapshotRequest) (*commonpb.Snapshot, error) {
	future := make(chan *commonpb.Snapshot)
	n.Raft.snapCh <- future

	select {
	case snapshot := <-future:
		n.catchUp <- &catchUpRequest{
			nextIndex:  snapshot.Index,
			followerID: req.FollowerID,
		}
		return snapshot, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type catchUpRequest struct {
	nextIndex  uint64
	followerID uint64
}

func (n *Node) doCatchUp(conf *gorums.Configuration, nextIndex uint64, matchIndex chan uint64) {
	for {
		state := n.Raft.State()

		if state != Leader {
			close(matchIndex)
			return
		}

		n.Raft.Lock()
		entries := n.Raft.getNextEntries(nextIndex)
		request := n.Raft.getAppendEntriesRequest(nextIndex, entries)
		n.Raft.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
		res, err := conf.AppendEntries(ctx, request)

		if err != nil {
			// TODO Better error message.
			log.Println(fmt.Sprintf("Catch-up AppendEntries failed = %v", err))

			if res.AppendEntriesResponse == nil {
				continue
			}
		}

		cancel()

		response := res.AppendEntriesResponse

		if response.Success {
			matchIndex <- response.MatchIndex
			index := <-matchIndex

			if response.MatchIndex == index {
				close(matchIndex)
				return
			}

			nextIndex = response.MatchIndex + 1

			continue
		}

		// If AppendEntries was unsuccessful a new catch-up process will
		// start.
		close(matchIndex)
		return
	}
}

func (n *Node) getNodeID(raftID uint64) uint32 {
	return n.mgr.NodeIDs()[n.lookup[raftID]]
}
