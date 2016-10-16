package raft

import "github.com/relab/raft/proto/gorums"

// QuorumSpec holds information about the quorum size of the current configuration
// and allows us to invoke QRPCs.
type QuorumSpec struct {
	N int
	Q int
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

		if votes >= qspec.Q {
			response.VoteGranted = true
			return response, true
		}
	}

	return response, false
}
