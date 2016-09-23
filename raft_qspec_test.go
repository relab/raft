package main

import (
	"reflect"
	"testing"

	"github.com/relab/raft/proto/gorums"
)

var requestVoteQFTests = []struct {
	name    string
	replies []*gorums.RequestVoteResponse
	quorum  bool
	reply   *gorums.RequestVoteResponse
}{
	{
		"no responses, len(replies) = 0",
		[]*gorums.RequestVoteResponse{},
		false,
		nil,
	},
	{
		"not enough responses, len(replies) < q",
		[]*gorums.RequestVoteResponse{
			{Term: 2, VoteGranted: true},
		},
		false,
		nil,
	},
	{
		"grant vote, len(replies) = q",
		[]*gorums.RequestVoteResponse{
			{Term: 3, VoteGranted: true},
			{Term: 3, VoteGranted: true},
		},
		true,
		&gorums.RequestVoteResponse{Term: 3, VoteGranted: true},
	},
	{
		"grant vote, len(replies) = n",
		[]*gorums.RequestVoteResponse{
			{Term: 4, VoteGranted: true},
			{Term: 4, VoteGranted: true},
			{Term: 4, VoteGranted: true},
		},
		true,
		&gorums.RequestVoteResponse{Term: 4, VoteGranted: true},
	},
	{
		"higher term, len(replies) = n",
		[]*gorums.RequestVoteResponse{
			{Term: 3, VoteGranted: true},
			{Term: 3, VoteGranted: true},
			{Term: 4, VoteGranted: false},
		},
		true,
		&gorums.RequestVoteResponse{Term: 4, VoteGranted: false},
	},
	{
		"diff terms, len(replies) = n",
		[]*gorums.RequestVoteResponse{
			{Term: 3, VoteGranted: false},
			{Term: 4, VoteGranted: false},
			{Term: 5, VoteGranted: false},
		},
		true,
		&gorums.RequestVoteResponse{Term: 5, VoteGranted: false},
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
		for _, test := range requestVoteQFTests {
			t.Run(qspec.name+"-"+test.name, func(t *testing.T) {
				reply, quorum := qspec.spec.RequestVoteQF(test.replies)

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
