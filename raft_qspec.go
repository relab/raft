package main

import "github.com/relab/raft/proto/gorums"

type raftQSpec struct {
	n int
	q int
}

func (qs *raftQSpec) RequestVoteQF(replies []*gorums.RequestVoteResponse) (*gorums.RequestVoteResponse, bool) {
	if len(replies) < qs.q {
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

	if votes >= qs.q {
		response.VoteGranted = true
	}

	return response, true
}

func (qs *raftQSpec) AppendEntriesQF(replies []*gorums.AppendEntriesResponse) (*gorums.AppendEntriesResponse, bool) {
	if len(replies) < qs.q {
		return nil, false
	}

	return replies[0], true
}
