package raft_test

import (
	"reflect"
	"testing"

	"github.com/relab/raft"
	gorums "github.com/relab/raft/proto/gorums"
	pb "github.com/relab/raft/proto/messages"
)

var requestVoteQFTests = []struct {
	name    string
	request *pb.RequestVoteRequest
	replies []*pb.RequestVoteResponse
	quorum  bool
	reply   *pb.RequestVoteResponse
}{
	{
		"do not grant vote",
		&pb.RequestVoteRequest{Term: 2},
		[]*pb.RequestVoteResponse{
			{Term: 2, VoteGranted: false},
		},
		false,
		nil,
	},
	{
		"grant vote",
		&pb.RequestVoteRequest{Term: 3},
		[]*pb.RequestVoteResponse{
			{Term: 3, VoteGranted: true},
		},
		true,
		&pb.RequestVoteResponse{Term: 3, VoteGranted: true},
	},
	{
		"reply with higher Term",
		&pb.RequestVoteRequest{Term: 3},
		[]*pb.RequestVoteResponse{
			{Term: 4, VoteGranted: false},
		},
		true,
		&pb.RequestVoteResponse{Term: 4, VoteGranted: false},
	},
}

type AETestCase struct {
	name    string
	request *pb.AppendEntriesRequest
	replies []*pb.AppendEntriesResponse
	quorum  bool
	reply   *pb.AppendEntriesResponse
}

var appendEntriesCommonQFTests = []AETestCase{
	{
		"reply with higher Term",
		&pb.AppendEntriesRequest{Term: 5},
		[]*pb.AppendEntriesResponse{
			{
				Term: 6,
			},
		},
		true,
		&pb.AppendEntriesResponse{
			Term: 6,
		},
	},
	{
		"one unsuccessful MatchIndex",
		&pb.AppendEntriesRequest{Term: 5},
		[]*pb.AppendEntriesResponse{
			{
				Term:       5,
				MatchIndex: 50,
				Success:    false,
			},
		},
		false,
		&pb.AppendEntriesResponse{
			Term:       5,
			MatchIndex: 50,
			Success:    false,
		},
	},
	{
		"two unsuccessful same MatchIndex",
		&pb.AppendEntriesRequest{Term: 5},
		[]*pb.AppendEntriesResponse{
			{
				Term:       5,
				MatchIndex: 100,
				Success:    false,
			},
			{
				Term:       5,
				MatchIndex: 100,
				Success:    false,
			},
		},
		false,
		&pb.AppendEntriesResponse{
			Term:       5,
			MatchIndex: 100,
			Success:    false,
		},
	},
	{
		"two unsuccessful different MatchIndex",
		&pb.AppendEntriesRequest{Term: 5},
		[]*pb.AppendEntriesResponse{
			{
				Term:       5,
				MatchIndex: 50,
				Success:    false,
			},
			{
				Term:       5,
				MatchIndex: 100,
				Success:    false,
			},
		},
		false,
		&pb.AppendEntriesResponse{
			Term:       5,
			MatchIndex: 50,
			Success:    false,
		},
	},
}

var appendEntriesFastQFTests = []AETestCase{
	// Stops after successful quorum, ignoring the last response.
	{
		"quorum successful",
		&pb.AppendEntriesRequest{Term: 5},
		[]*pb.AppendEntriesResponse{
			{
				Term:       5,
				MatchIndex: 100,
				Success:    true,
			},
		},
		true,
		&pb.AppendEntriesResponse{
			Term:       5,
			MatchIndex: 100,
			Success:    true,
		},
	},
	// Stops after successful quorum, ignoring the last response.
	// This tests shows how one follower is left behind, as from this point
	// it will never receive updates as long as the first follower is faster.
	{
		"one of two successful MatchIndex",
		&pb.AppendEntriesRequest{Term: 5},
		[]*pb.AppendEntriesResponse{
			{
				Term:       5,
				MatchIndex: 100,
				Success:    true,
			},
			{
				Term:       5,
				MatchIndex: 50,
				Success:    false,
			},
		},
		true,
		&pb.AppendEntriesResponse{
			Term:       5,
			MatchIndex: 100,
			Success:    true,
		},
	},
}

var qspecs = []struct {
	name string
	spec gorums.QuorumSpec
}{
	{
		"QuorumSpec N3 Q1",
		&raft.QuorumSpec{N: 2, Q: 1},
	},
}

func TestRequestVoteQF(t *testing.T) {
	for _, qs := range qspecs {
		for _, test := range requestVoteQFTests {
			t.Run(qs.name+"-"+test.name, func(t *testing.T) {
				reply, quorum := qs.spec.RequestVoteQF(test.request, test.replies)

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

func TestAppendEntriesFastQF(t *testing.T) {
	for _, qs := range qspecs {
		for _, test := range append(appendEntriesCommonQFTests, appendEntriesFastQFTests...) {
			t.Run(qs.name+"-"+test.name, func(t *testing.T) {
				reply, quorum := qs.spec.AppendEntriesQF(test.request, test.replies)

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
