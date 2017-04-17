package raftgorums_test

import (
	"reflect"
	"testing"

	"github.com/relab/raft/raftgorums"
	gorums "github.com/relab/raft/raftgorums/gorumspb"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

var requestVoteQFTests = []struct {
	name    string
	qs      spec
	request *pb.RequestVoteRequest
	replies []*pb.RequestVoteResponse
	quorum  bool
	reply   *pb.RequestVoteResponse
}{
	{
		"do not grant vote, single reply",
		n3q1,
		&pb.RequestVoteRequest{Term: 2},
		[]*pb.RequestVoteResponse{
			{Term: 2, VoteGranted: false},
		},
		false,
		nil,
	},
	{
		"do not grant vote, all replies",
		n3q1,
		&pb.RequestVoteRequest{Term: 2},
		[]*pb.RequestVoteResponse{
			{Term: 2, VoteGranted: false},
			{Term: 2, VoteGranted: false},
		},
		true,
		&pb.RequestVoteResponse{Term: 2},
	},
	{
		"grant vote",
		n3q1,
		&pb.RequestVoteRequest{Term: 3},
		[]*pb.RequestVoteResponse{
			{Term: 3, VoteGranted: true},
		},
		true,
		&pb.RequestVoteResponse{Term: 3, VoteGranted: true},
	},
	{
		"reply with higher Term",
		n3q1,
		&pb.RequestVoteRequest{Term: 3},
		[]*pb.RequestVoteResponse{
			{Term: 4, VoteGranted: false},
		},
		true,
		&pb.RequestVoteResponse{Term: 4, VoteGranted: false},
	},
}

var appendEntriesQFTests = []struct {
	name    string
	qs      spec
	request *pb.AppendEntriesRequest
	replies []*pb.AppendEntriesResponse
	quorum  bool
	reply   *pb.AppendEntriesQFResponse
}{
	{
		"reply with higher Term",
		n3q1,
		&pb.AppendEntriesRequest{Term: 5},
		[]*pb.AppendEntriesResponse{
			{
				Term: 6,
			},
		},
		true,
		&pb.AppendEntriesQFResponse{
			Term:    6,
			Replies: 1,
		},
	},
	{
		"one unsuccessful MatchIndex",
		n3q1,
		&pb.AppendEntriesRequest{Term: 5},
		[]*pb.AppendEntriesResponse{
			{
				Term:       5,
				MatchIndex: 50,
				Success:    false,
			},
		},
		false,
		&pb.AppendEntriesQFResponse{
			Term:       5,
			MatchIndex: 50,
			Replies:    1,
			Success:    false,
		},
	},
	{
		"two unsuccessful same MatchIndex",
		n3q1,
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
		true,
		&pb.AppendEntriesQFResponse{
			Term:       5,
			MatchIndex: 100,
			Replies:    2,
			Success:    false,
		},
	},
	{
		"two unsuccessful different MatchIndex",
		n3q1,
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
		true,
		&pb.AppendEntriesQFResponse{
			Term:       5,
			Replies:    2,
			MatchIndex: 50,
			Success:    false,
		},
	},
	{
		"quorum successful",
		n3q1,
		&pb.AppendEntriesRequest{Term: 5},
		[]*pb.AppendEntriesResponse{
			{
				Term:       5,
				MatchIndex: 100,
				Success:    true,
			},
		},
		true,
		&pb.AppendEntriesQFResponse{
			Term:       5,
			MatchIndex: 100,
			Replies:    1,
			Success:    true,
		},
	},
}

type spec struct {
	name string
	qs   gorums.QuorumSpec
}

var (
	n1q1 = spec{
		"QuorumSpec N1 Q1",
		raftgorums.NewQuorumSpec(3),
	}
	n2q1 = spec{
		"QuorumSpec N2 Q1",
		raftgorums.NewQuorumSpec(3),
	}
	n3q1 = spec{
		"QuorumSpec N3 Q1",
		raftgorums.NewQuorumSpec(3),
	}
	n7q4 = spec{
		"QuorumSpec N7 Q4",
		raftgorums.NewQuorumSpec(3),
	}
)

func TestRequestVoteQF(t *testing.T) {
	for _, test := range requestVoteQFTests {
		t.Run(test.qs.name+"-"+test.name, func(t *testing.T) {
			reply, quorum := test.qs.qs.RequestVoteQF(test.request, test.replies)

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
	for _, test := range appendEntriesQFTests {
		t.Run(test.qs.name+"-"+test.name, func(t *testing.T) {
			reply, quorum := test.qs.qs.AppendEntriesQF(test.request, test.replies)

			if quorum != test.quorum {
				t.Errorf("got %t, want %t", quorum, test.quorum)
			}

			if !reflect.DeepEqual(reply, test.reply) {
				t.Errorf("got %+v, want %+v", reply, test.reply)
			}
		})
	}
}
