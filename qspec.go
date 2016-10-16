package raft

import "github.com/relab/raft/proto/gorums"

type QuorumSpec struct {
	N int
	Q int
}

func (qspec *QuorumSpec) RequestVoteQF(replies []*gorums.RequestVoteResponse) (*gorums.RequestVoteResponse, bool) {
	votes := 0
	response := &gorums.RequestVoteResponse{Term: replies[0].Term}

	for _, reply := range replies {
		if reply.Term != response.Term {
			response.Term = max(response.Term, reply.Term)

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
