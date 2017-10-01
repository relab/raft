package raftgorums_test

import (
	"reflect"
	"testing"

	"github.com/relab/raft/raftgorums"
	gorums "github.com/relab/raft/raftgorums/gorumspb"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

func TestNewRaftQuorumSpec(t *testing.T) {
	minTests := []struct {
		name  string
		peers int
		n, q  int
	}{
		{"3 peers", 3, 2, 1},
		{"4 peers", 4, 3, 2},
		{"5 peers", 5, 4, 2},
		{"6 peers", 6, 5, 3},
		{"7 peers", 7, 6, 3},
	}

	for _, test := range minTests {
		t.Run(test.name, func(t *testing.T) {
			qs := raftgorums.NewQuorumSpec(test.peers)

			if qs.N != test.n {
				t.Errorf("got %d, want %d", qs.N, test.n)
			}

			if qs.Q != test.q {
				t.Errorf("got %d, want %d", qs.Q, test.q)
			}
		})
	}
}

var requestVoteQFTests = []struct {
	name    string
	request *pb.RequestVoteRequest
	replies []*pb.RequestVoteResponse
	quorum  bool
	reply   *pb.RequestVoteResponse
}{
	{
		"do not grant vote, single reply",
		&pb.RequestVoteRequest{Term: 2},
		[]*pb.RequestVoteResponse{
			{Term: 2, VoteGranted: false},
		},
		false,
		nil,
	},
	{
		"do not grant vote, all replies",
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

var appendEntriesQFTests = []struct {
	name    string
	request *pb.AppendEntriesRequest
	replies []*pb.AppendEntriesResponse
	quorum  bool
	reply   *pb.AppendEntriesQFResponse
}{
	{
		"reply with higher Term",
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

var specs = []struct {
	name string
	qs   gorums.QuorumSpec
}{
	{
		"QuorumSpec N1 Q1",
		raftgorums.NewQuorumSpec(3),
	},
	{
		"QuorumSpec N2 Q1",
		raftgorums.NewQuorumSpec(3),
	},
	{
		"QuorumSpec N3 Q1",
		raftgorums.NewQuorumSpec(3),
	},
	{
		"QuorumSpec N7 Q4",
		raftgorums.NewQuorumSpec(3),
	},
}

func TestRequestVoteQF(t *testing.T) {
	for _, test := range requestVoteQFTests {
		for _, spec := range specs {
			t.Run(spec.name+"-"+test.name, func(t *testing.T) {
				reply, quorum := spec.qs.RequestVoteQF(test.request, test.replies)

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
	for _, test := range appendEntriesQFTests {
		for _, spec := range specs {
			t.Run(spec.name+"-"+test.name, func(t *testing.T) {
				reply, quorum := spec.qs.AppendEntriesQF(test.request, test.replies)

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
