package raftgorums

import (
	"math"

	pb "github.com/relab/raft/raftgorums/raftpb"
)

// QuorumSpec holds information about the quorum size of the current
// configuration and allows us to invoke QRPCs.
// TODO Rename so it's not confused with gorums.QuorumSpec
type QuorumSpec struct {
	N int
	Q int
}

// RequestVoteQF gathers RequestVoteResponses and delivers a reply when a higher
// term is seen or a quorum of votes is received.
// TODO Implements gorums.QuorumSpec interface.
func (qs *QuorumSpec) RequestVoteQF(req *pb.RequestVoteRequest, replies []*pb.RequestVoteResponse) (*pb.RequestVoteResponse, bool) {
	// Make copy of last reply.
	response := *replies[len(replies)-1]

	if response.Term > req.Term {
		// Abort.
		return &response, true
	}

	// Being past this point means last.Term == req.Term.

	var votes int

	for _, reply := range replies {
		// Tally votes.
		if reply.VoteGranted {
			votes++
		}

	}

	if votes >= qs.Q {
		// Quorum.
		response.VoteGranted = true
		return &response, true
	}

	if len(replies) < qs.N {
		// Wait for more replies.
		return nil, false
	}

	return nil, true
}

// AppendEntriesQF gathers AppendEntriesResponses and calculates the log entries
// replicated, depending on the quorum configuration.
// TODO Implements gorums.QuorumSpec interface.
func (qs *QuorumSpec) AppendEntriesQF(req *pb.AppendEntriesRequest, replies []*pb.AppendEntriesResponse) (*pb.AppendEntriesResponse, bool) {
	// Make copy of last reply.
	response := *replies[len(replies)-1]

	if response.Term > req.Term {
		// Abort.
		return &response, true
	}

	// Being past this point means last.Term == req.Term.

	var successful int
	var minMatch uint64 = math.MaxUint64
	response.Success = false // Default to unsuccessful.

	for _, r := range replies {
		// Track lowest match index.
		if r.MatchIndex < minMatch {
			minMatch = r.MatchIndex
		}

		// Count successful.
		if r.Success {
			response.MatchIndex = r.MatchIndex
			successful++
		}
	}

	if successful >= qs.Q {
		// Quorum.
		response.Success = true
		return &response, true
	}

	response.MatchIndex = minMatch

	// Wait for more replies. Return response, even on failure. This allows
	// raft to back off and try a lower match index.

	if len(replies) < qs.N {
		return &response, false
	}

	return &response, true
}
