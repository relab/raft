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
	// Make copy of last reply.
	response := *replies[len(replies)-1]

	// On abort.
	if response.Term > req.Term {
		return &response, true
	}

	// Being past this point means last.Term == req.Term.

	var votes int

	// Tally votes.
	for _, reply := range replies {
		if reply.VoteGranted {
			votes++
		}

	}

	// On quorum.
	if votes >= qs.Q {
		response.VoteGranted = true
		return &response, true
	}

	// Wait for more replies.
	return nil, false
}

// AppendEntriesQF gathers AppendEntriesResponses and calculates the log entries
// replicated, depending on the quorum configuration.
func (qs *QuorumSpec) AppendEntriesQF(req *pb.AppendEntriesRequest, replies []*pb.AppendEntriesResponse) (*pb.AppendEntriesResponse, bool) {
	// Make copy of last reply.
	response := *replies[len(replies)-1]

	// On abort.
	if response.Term > req.Term {
		return &response, true
	}

	// Being past this point means last.Term == req.Term.

	var successful int
	minMatch := ^uint64(0)   // Largest uint64.
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

	// On quorum.
	if successful >= qs.Q {
		response.Success = true
		return &response, true
	}

	response.MatchIndex = minMatch

	// Wait for more replies. Return response, even on failure. This allows
	// raft to back off and try a lower match index.
	return &response, false
}
