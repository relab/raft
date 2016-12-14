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

		if votes >= qspec.SQ {
			response.VoteGranted = true
			return response, true
		}
	}

	return response, false
}

// TODO GoDoc
func (qspec *QuorumSpec) AppendEntriesQF(replies []*gorums.AppendEntriesResponse) (*gorums.AppendEntriesResponse, bool) {
	numSuccess := 0
	response := &gorums.AppendEntriesResponse{}

	for _, reply := range replies {
		if reply.MatchIndex < response.MatchIndex || response.MatchIndex == 0 {
			response.MatchIndex = reply.MatchIndex
		}

		if reply.Term > response.Term || response.Term == 0 {
			response.Term = reply.Term
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

	// If some replicas are down, we still want the cluster to function.
	response.Success = numSuccess >= qspec.FQ && len(replies) == numSuccess

	return response, len(replies) == qspec.N-1
}
