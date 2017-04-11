package raftgorums

import (
	"golang.org/x/net/context"

	"github.com/relab/raft/commonpb"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

// RequestVote implements gorums.RaftServer.
func (r *Raft) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return r.HandleRequestVoteRequest(req), nil
}

// AppendEntries implements gorums.RaftServer.
func (r *Raft) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return r.HandleAppendEntriesRequest(req), nil
}

// InstallSnapshot implements gorums.RaftServer.
func (r *Raft) InstallSnapshot(ctx context.Context, snapshot *commonpb.Snapshot) (*pb.InstallSnapshotResponse, error) {
	return r.HandleInstallSnapshotRequest(snapshot), nil
}

// CatchMeUp implements gorums.RaftServer.
func (r *Raft) CatchMeUp(ctx context.Context, req *pb.CatchMeUpRequest) (res *pb.Empty, err error) {
	res = &pb.Empty{}
	r.match[r.getNodeID(req.FollowerID)] <- req.NextIndex
	return
}
