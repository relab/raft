package raft

import pb "github.com/relab/raft/pkg/raft/raftpb"

// QuorumSpec holds information about the quorum size of the current
// configuration and allows us to invoke QRPCs.
type QuorumSpec struct {
	N int
	Q int
}

// RequestVoteQF gathers RequestVoteResponses and delivers a reply when a higher
// term is seen or a quorum of votes is received.
func (qs *QuorumSpec) RequestVoteQF(req *pb.RequestVoteRequest, replies []*pb.RequestVoteResponse) (*pb.RequestVoteResponse, bool) {
	votes := 0
	last := replies[len(replies)-1]

	if last.Term > req.Term {
		return last, true
	}

	for _, reply := range replies {
		if reply.VoteGranted {
			votes++
		}

	}

	if votes >= qs.Q {
		last.VoteGranted = true

		return last, true
	}

	return nil, false
}

// AppendEntriesQF gathers AppendEntriesResponses and calculates the log entries
// replicated, depending on the quorum configuration.
func (qs *QuorumSpec) AppendEntriesQF(req *pb.AppendEntriesRequest, replies []*pb.AppendEntriesResponse) (*pb.AppendEntriesResponse, bool) {
	last := replies[len(replies)-1]

	// Abort.
	if last.Term > req.Term {
		return last, true
	}

	successful := 0
	minMatch := ^uint64(0) // Largest uint64.
	reply := &pb.AppendEntriesResponse{Term: req.Term}

	for _, r := range replies {
		if r.MatchIndex < minMatch {
			minMatch = r.MatchIndex
		}

		if r.Success {
			reply.MatchIndex = r.MatchIndex
			successful++
		}
	}

	// Quorum.
	if successful >= qs.Q {
		reply.Success = true
		return reply, true
	}

	// Set match index to the lowest seen.
	reply.MatchIndex = minMatch

	// Wait for more replies.
	return reply, len(replies) == qs.N
}
