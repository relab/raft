package raft

import "github.com/relab/raft/proto/gorums"

type QuorumSpec struct {
	N int
	Q int
}

func (qspec *QuorumSpec) RequestVoteQF(replies []*gorums.RequestVoteResponse) (*gorums.RequestVoteResponse, bool) {
	if len(replies) < qspec.Q {
		return nil, false
	}

	var term uint64

	for _, reply := range replies {
		if reply.Term > term {
			term = reply.Term
		}
	}

	votes := 0

	for _, reply := range replies {
		if reply.Term == term && reply.VoteGranted {
			votes++
		}
	}

	response := &gorums.RequestVoteResponse{Term: term}

	if votes >= qspec.Q {
		response.VoteGranted = true
	}

	return response, true
}

func (qspec *QuorumSpec) AppendEntriesQF(replies []*gorums.AppendEntriesResponse) (*gorums.AppendEntriesResponse, bool) {
	if len(replies) < 1 {
		return nil, false
	}

	return replies[0], true
}
