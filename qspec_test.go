package raft_test

import (
	"reflect"
	"testing"

	"github.com/relab/raft"
	"github.com/relab/raft/proto/gorums"
)

/*
Note that it follows from the Raft specification that a response to a request
sent in term RequestTerm, will always contain a Term >= RequestTerm. This is the
reason there are no tests with Term < RequestTerm, as a follower is required to
update its Term to RequestTerm if its current term is lower. Also note that a
RequestTerm will always be the same for all responses to a given quorum call,
this follows from the fact that the field is set directly from the request
received.
*/

var requestVoteQFTests = []struct {
	name    string
	replies []*gorums.RequestVoteResponse
	quorum  bool
	reply   *gorums.RequestVoteResponse
}{
	{
		"do not grant vote",
		[]*gorums.RequestVoteResponse{
			{Term: 2, RequestTerm: 2, VoteGranted: false},
		},
		false,
		nil,
	},
	{
		"grant vote",
		[]*gorums.RequestVoteResponse{
			{Term: 3, RequestTerm: 3, VoteGranted: true},
		},
		true,
		&gorums.RequestVoteResponse{Term: 3, RequestTerm: 3, VoteGranted: true},
	},
	{
		"reply higher term",
		[]*gorums.RequestVoteResponse{
			{Term: 4, RequestTerm: 3, VoteGranted: false},
		},
		true,
		&gorums.RequestVoteResponse{Term: 4, RequestTerm: 3, VoteGranted: false},
	},
}

var appendEntriesCommonQFTests = []struct {
	name    string
	replies []*gorums.AppendEntriesResponse
	quorum  bool
	reply   *gorums.AppendEntriesResponse
}{
	{
		"reply with higher Term",
		[]*gorums.AppendEntriesResponse{
			{
				FollowerID:  []uint32{10},
				RequestTerm: 5,
				Term:        6,
			},
		},
		true,
		&gorums.AppendEntriesResponse{
			RequestTerm: 5,
			Term:        6,
		},
	},
	{
		"one unsuccessful MatchIndex",
		[]*gorums.AppendEntriesResponse{
			{
				FollowerID:  []uint32{10},
				RequestTerm: 5,
				Term:        5,
				MatchIndex:  50,
				Success:     false,
			},
		},
		false,
		&gorums.AppendEntriesResponse{
			RequestTerm: 5,
			Term:        5,
			MatchIndex:  50,
			Success:     false,
		},
	},
	{
		"two unsuccessful same MatchIndex",
		[]*gorums.AppendEntriesResponse{
			{
				FollowerID:  []uint32{10},
				RequestTerm: 5,
				Term:        5,
				MatchIndex:  100,
				Success:     false,
			},
			{
				FollowerID:  []uint32{20},
				RequestTerm: 5,
				Term:        5,
				MatchIndex:  100,
				Success:     false,
			},
		},
		false,
		&gorums.AppendEntriesResponse{
			RequestTerm: 5,
			Term:        5,
			MatchIndex:  100,
			Success:     false,
		},
	},
	{
		"two unsuccessful different MatchIndex",
		[]*gorums.AppendEntriesResponse{
			{
				FollowerID:  []uint32{10},
				RequestTerm: 5,
				Term:        5,
				MatchIndex:  50,
				Success:     false,
			},
			{
				FollowerID:  []uint32{20},
				RequestTerm: 5,
				Term:        5,
				MatchIndex:  100,
				Success:     false,
			},
		},
		false,
		&gorums.AppendEntriesResponse{
			RequestTerm: 5,
			Term:        5,
			MatchIndex:  50,
			Success:     false,
		},
	},
}

var appendEntriesFastQFTests = []struct {
	name    string
	replies []*gorums.AppendEntriesResponse
	quorum  bool
	reply   *gorums.AppendEntriesResponse
}{
	// Stops after successful quorum, ignoring the last response.
	{
		"quorum successful",
		[]*gorums.AppendEntriesResponse{
			{
				FollowerID:  []uint32{10},
				RequestTerm: 5,
				Term:        5,
				MatchIndex:  100,
				Success:     true,
			},
		},
		true,
		&gorums.AppendEntriesResponse{
			FollowerID:  []uint32{10},
			RequestTerm: 5,
			Term:        5,
			MatchIndex:  100,
			Success:     true,
		},
	},
	// Stops after successful quorum, ignoring the last response.
	// This tests shows how one follower is left behind, as from this point
	// it will never receive updates as long as the first follower is faster.
	{
		"one of two successful MatchIndex",
		[]*gorums.AppendEntriesResponse{
			{
				FollowerID:  []uint32{10},
				RequestTerm: 5,
				Term:        5,
				MatchIndex:  100,
				Success:     true,
			},
			{
				FollowerID:  []uint32{20},
				RequestTerm: 5,
				Term:        5,
				MatchIndex:  50,
				Success:     false,
			},
		},
		true,
		&gorums.AppendEntriesResponse{
			FollowerID:  []uint32{10},
			RequestTerm: 5,
			Term:        5,
			MatchIndex:  100,
			Success:     true,
		},
	},
}

var appendEntriesSlowQFTests = []struct {
	name    string
	replies []*gorums.AppendEntriesResponse
	quorum  bool
	reply   *gorums.AppendEntriesResponse
}{
	// Reads all responses.
	{
		"two successful same MatchIndex",
		[]*gorums.AppendEntriesResponse{
			{
				FollowerID:  []uint32{10},
				RequestTerm: 5,
				Term:        5,
				MatchIndex:  100,
				Success:     true,
			},
			{
				FollowerID:  []uint32{20},
				RequestTerm: 5,
				Term:        5,
				MatchIndex:  100,
				Success:     true,
			},
		},
		true,
		&gorums.AppendEntriesResponse{
			FollowerID:  []uint32{10, 20},
			RequestTerm: 5,
			Term:        5,
			MatchIndex:  100,
			Success:     true,
		},
	},
	// Reads all responses, returning with the lowest MatchIndex.
	// This tests show how this configuration makes sure all followers are up-to-date.
	{
		"one of two successful MatchIndex",
		[]*gorums.AppendEntriesResponse{
			{
				FollowerID:  []uint32{10},
				RequestTerm: 5,
				Term:        5,
				MatchIndex:  100,
				Success:     true,
			},
			{
				FollowerID:  []uint32{20},
				RequestTerm: 5,
				Term:        5,
				MatchIndex:  50,
				Success:     false,
			},
		},
		false,
		&gorums.AppendEntriesResponse{
			RequestTerm: 5,
			Term:        5,
			MatchIndex:  50,
			Success:     false,
		},
	},
}

var qspecs = []struct {
	name string
	spec gorums.QuorumSpec
}{
	{
		"QuorumSpec N3 FQ1",
		&raft.QuorumSpec{N: 3, FQ: 1},
	},
	{
		"QuorumSpec N3 FQ1 SQ1",
		&raft.QuorumSpec{N: 3, FQ: 1, SQ: 1},
	},
	{
		"QuorumSpec N3 FQ1 SQ2",
		&raft.QuorumSpec{N: 3, FQ: 1, SQ: 2},
	},
}

func TestRequestVoteQF(t *testing.T) {
	qspec := qspecs[0]

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

func TestAppendEntriesFastQF(t *testing.T) {
	qspec := qspecs[1]

	for _, test := range append(appendEntriesCommonQFTests, appendEntriesFastQFTests...) {
		t.Run(qspec.name+"-"+test.name, func(t *testing.T) {
			reply, quorum := qspec.spec.AppendEntriesQF(test.replies)

			if quorum != test.quorum {
				t.Errorf("got %t, want %t", quorum, test.quorum)
			}

			if !reflect.DeepEqual(reply, test.reply) {
				t.Errorf("got %+v, want %+v", reply, test.reply)
			}
		})
	}
}

func TestAppendEntriesSlowQF(t *testing.T) {
	qspec := qspecs[2]

	for _, test := range append(appendEntriesCommonQFTests, appendEntriesSlowQFTests...) {
		t.Run(qspec.name+"-"+test.name, func(t *testing.T) {
			reply, quorum := qspec.spec.AppendEntriesQF(test.replies)

			if quorum != test.quorum {
				t.Errorf("got %t, want %t", quorum, test.quorum)
			}

			if !reflect.DeepEqual(reply, test.reply) {
				t.Errorf("got %+v, want %+v", reply, test.reply)
			}
		})
	}
}
