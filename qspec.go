package raft

import "github.com/relab/raft/proto/gorums"

// QuorumSpec holds information about the quorum size of the current configuration
// and allows us to invoke QRPCs.
type QuorumSpec struct {
	N  int
	SQ int
	FQ int
}

// RequestVoteQF gathers RequestVoteResponses
// and delivers a reply when a higher term is seen or a quorum of votes is received.
func (qspec *QuorumSpec) RequestVoteQF(replies []*gorums.RequestVoteResponse) (*gorums.RequestVoteResponse, bool) {
	votes := 0
	response := &gorums.RequestVoteResponse{Term: replies[0].RequestTerm}

	for _, reply := range replies {
		if reply.Term > response.Term {
			response.Term = reply.Term

			return response, true
		}

		if reply.VoteGranted {
			votes++
		}

		if votes >= qspec.FQ {
			response.VoteGranted = true
			return response, true
		}
	}

	return nil, false
}

// AppendEntriesQF gathers AppendEntriesResponses
// and calculates the log entries replicated, depending on the quorum configuration.
func (qspec *QuorumSpec) AppendEntriesQF(replies []*gorums.AppendEntriesResponse) (*gorums.AppendEntriesResponse, bool) {
	numSuccess := 0
	response := &gorums.AppendEntriesResponse{Term: replies[0].RequestTerm}

	for _, reply := range replies {
		if reply.MatchIndex < response.MatchIndex || response.MatchIndex == 0 {
			response.MatchIndex = reply.MatchIndex
		}

		if reply.Term > response.Term {
			response.Term = reply.Term

			return response, true
		}

		if reply.Success {
			numSuccess++
			response.FollowerID = append(response.FollowerID, reply.FollowerID[0])
		}

		if numSuccess >= qspec.SQ {
			reply.FollowerID = response.FollowerID

			return reply, true
		}
	}

	// Majority quorum.
	quorum := numSuccess >= qspec.FQ

	// If we have a majority and all the replicas that responded were successful.
	if quorum && len(replies) == numSuccess {
		response.Success = true
	} else {
		// This is not needed but makes it clear that FollowerID should
		// not be used when AppendEntries fail.
		response.FollowerID = nil
	}

	// If all replicas have responded.
	if len(replies) == qspec.N-1 {
		return response, true
	}

	// Wait for more replies but leave the response in the case of a timeout.
	// We do this so that the cluster can proceed even if some replicas are down.
	return response, false
}
