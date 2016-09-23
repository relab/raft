package main

import (
	"reflect"
	"testing"

	"github.com/relab/raft/proto/gorums"
)

var reqVoteQFTests = []struct {
	name    string
	replies []*gorums.RequestVote_Response
	quorum  bool
	reply   *gorums.RequestVote_Response
}{
	{
		"no responses, len(replies) = 0",
		[]*gorums.RequestVote_Response{},
		false,
		nil,
	},
	{
		"not enough responses, len(replies) < q",
		[]*gorums.RequestVote_Response{
			{Term: 2, VoteGranted: true},
		},
		false,
		nil,
	},
	{
		"grant vote, len(replies) = q",
		[]*gorums.RequestVote_Response{
			{Term: 3, VoteGranted: true},
			{Term: 3, VoteGranted: true},
		},
		true,
		&gorums.RequestVote_Response{Term: 3, VoteGranted: true},
	},
	{
		"grant vote, len(replies) = n",
		[]*gorums.RequestVote_Response{
			{Term: 4, VoteGranted: true},
			{Term: 4, VoteGranted: true},
			{Term: 4, VoteGranted: true},
		},
		true,
		&gorums.RequestVote_Response{Term: 4, VoteGranted: true},
	},
	{
		"higher term, len(replies) = n",
		[]*gorums.RequestVote_Response{
			{Term: 3, VoteGranted: true},
			{Term: 3, VoteGranted: true},
			{Term: 4, VoteGranted: false},
		},
		true,
		&gorums.RequestVote_Response{Term: 4, VoteGranted: false},
	},
	{
		"diff terms, len(replies) = n",
		[]*gorums.RequestVote_Response{
			{Term: 3, VoteGranted: false},
			{Term: 4, VoteGranted: false},
			{Term: 5, VoteGranted: false},
		},
		true,
		&gorums.RequestVote_Response{Term: 5, VoteGranted: false},
	},
}

var qspecs = []struct {
	name string
	spec gorums.QuorumSpec
}{
	{
		"raftQspec n:3, q:2",
		&raftQSpec{
			n: 3,
			q: 2,
		},
	},
}

func TestReqVoteQF(t *testing.T) {
	for _, qspec := range qspecs {
		for _, test := range reqVoteQFTests {
			t.Run(qspec.name+"-"+test.name, func(t *testing.T) {
				reply, quorum := qspec.spec.ReqVoteQF(test.replies)

				if quorum != test.quorum {
					t.Errorf("got %t, want %t", quorum, test.quorum)
				}

				if !reflect.DeepEqual(reply, test.reply) {
					t.Errorf("got %+v, want %+v", reply, test.reply)
				}
			})
		}
	}
}
