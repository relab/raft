package raftgorums

import (
	"fmt"
	"log"
	"net"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	gorums "github.com/relab/raft/raftgorums/gorumspb"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

// Node ties an instance of Raft to the network.
type Node struct {
	r *Raft

	server *grpc.Server

	addr  string
	peers []string

	conf *gorums.Configuration
}

// NewNode returns a Node with an instance of Raft given the configuration.
func NewNode(cfg *Config) *Node {
	peers := make([]string, len(cfg.Nodes))
	// We don't want to mutate cfg.Nodes.
	copy(peers, cfg.Nodes)

	id := cfg.ID
	addr := cfg.Nodes[id-1]
	// Exclude self.
	peers = append(peers[:id-1], peers[id:]...)

	n := &Node{
		r:      NewRaft(cfg),
		server: grpc.NewServer(),
		addr:   addr,
		peers:  peers,
	}

	gorums.RegisterRaftServer(n.server, n)

	return n
}

// Run start listening for incoming messages, and delivers outgoing messages.
func (n *Node) Run() (ferr error) {
	lis, err := net.Listen("tcp", n.addr)

	if err != nil {
		return err
	}

	go func() {
		ferr = n.server.Serve(lis)
	}()

	opts := []gorums.ManagerOption{
		gorums.WithGrpcDialOptions(
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(TCPConnect*time.Millisecond)),
		// TODO WithLogger?
	}

	mgr, err := gorums.NewManager(n.peers, opts...)

	if err != nil {
		return err
	}

	n.conf, err = mgr.NewConfiguration(mgr.NodeIDs(), NewQuorumSpec(len(n.peers)+1))

	if err != nil {
		return err
	}

	go n.r.Run()

	for {
		rvreqout := n.r.RequestVoteRequestChan()
		aereqout := n.r.AppendEntriesRequestChan()

		select {
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

			n.r.HandleRequestVoteResponse(res.RequestVoteResponse)
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

			n.r.HandleAppendEntriesResponse(res.AppendEntriesResponse)
		}
	}
}

// RequestVote implements gorums.RaftServer.
func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return n.r.HandleRequestVoteRequest(req), nil
}

// AppendEntries implements gorums.RaftServer.
func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return n.r.HandleAppendEntriesRequest(req), nil
}

// ClientCommand implements gorums.RaftServer.
func (n *Node) ClientCommand(ctx context.Context, req *pb.ClientCommandRequest) (*pb.ClientCommandResponse, error) {
	return n.r.HandleClientCommandRequest(req)
}
