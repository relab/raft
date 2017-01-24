package main

import (
	"golang.org/x/net/context"

	"github.com/relab/raft"
	pb "github.com/relab/raft/raftpb"
)

// Server wraps Replica and implements gorums.RaftServer.
type Server struct {
	*raft.Replica
}

// RequestVote implements gorums.RaftServer.
func (r *Server) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return r.HandleRequestVoteRequest(req), nil
}

// AppendEntries implements gorums.RaftServer.
func (r *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return r.HandleAppendEntriesRequest(req), nil
}

// ClientCommand implements gorums.RaftServer.
func (r *Server) ClientCommand(ctx context.Context, req *pb.ClientCommandRequest) (*pb.ClientCommandResponse, error) {
	return r.HandleClientCommandRequest(req)
}
