package main

import "github.com/relab/raft/proto/gorums"

type raftQSpec struct {
	n int
	q int
}

func (qs *raftQSpec) ReqVoteQF(replies []*gorums.RequestVote_Response) (*gorums.RequestVote_Response, bool) {
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

	response := &gorums.RequestVote_Response{Term: term}

	if votes >= qs.q {
		response.VoteGranted = true
	}

	return response, true
}

func (qs *raftQSpec) AppEntriesQF(replies []*gorums.AppendEntries_Response) (*gorums.AppendEntries_Response, bool) {
	if len(replies) < qs.q {
		return nil, false
	}

	return replies[0], true
}
