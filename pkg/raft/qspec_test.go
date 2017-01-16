package raft_test

import (
	"reflect"
	"testing"

	"github.com/relab/raft/pkg/raft"
	pb "github.com/relab/raft/pkg/raft/raftpb"
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
				FollowerID: []uint64{10},
				Term:       6,
			},
		},
		true,
		&pb.AppendEntriesResponse{
			FollowerID: []uint64{10},
			Term:       6,
		},
	},
	{
		"one unsuccessful MatchIndex",
		&pb.AppendEntriesRequest{Term: 5},
		[]*pb.AppendEntriesResponse{
			{
				FollowerID: []uint64{10},
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
				FollowerID: []uint64{10},
				Term:       5,
				MatchIndex: 100,
				Success:    false,
			},
			{
				FollowerID: []uint64{20},
				Term:       5,
				MatchIndex: 100,
				Success:    false,
			},
		},
		true,
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
				FollowerID: []uint64{10},
				Term:       5,
				MatchIndex: 50,
				Success:    false,
			},
			{
				FollowerID: []uint64{20},
				Term:       5,
				MatchIndex: 100,
				Success:    false,
			},
		},
		true,
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
				FollowerID: []uint64{10},
				Term:       5,
				MatchIndex: 100,
				Success:    true,
			},
		},
		true,
		&pb.AppendEntriesResponse{
			FollowerID: []uint64{10},
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
				FollowerID: []uint64{10},
				Term:       5,
				MatchIndex: 100,
				Success:    true,
			},
			{
				FollowerID: []uint64{20},
				Term:       5,
				MatchIndex: 50,
				Success:    false,
			},
		},
		true,
		&pb.AppendEntriesResponse{
			FollowerID: []uint64{10},
			Term:       5,
			MatchIndex: 100,
			Success:    true,
		},
	},
}

var appendEntriesSlowQFTests = []AETestCase{
	// Reads all responses.
	{
		"two successful same MatchIndex",
		&pb.AppendEntriesRequest{Term: 5},
		[]*pb.AppendEntriesResponse{
			{
				FollowerID: []uint64{10},
				Term:       5,
				MatchIndex: 100,
				Success:    true,
			},
			{
				FollowerID: []uint64{20},
				Term:       5,
				MatchIndex: 100,
				Success:    true,
			},
		},
		true,
		&pb.AppendEntriesResponse{
			FollowerID: []uint64{10, 20},
			Term:       5,
			MatchIndex: 100,
			Success:    true,
		},
	},
	// Reads all responses, returning with the lowest MatchIndex.
	// This tests show how this configuration makes sure all followers are up-to-date.
	{
		"one of two successful MatchIndex",
		&pb.AppendEntriesRequest{Term: 5},
		[]*pb.AppendEntriesResponse{
			{
				FollowerID: []uint64{10},
				Term:       5,
				MatchIndex: 100,
				Success:    true,
			},
			{
				FollowerID: []uint64{20},
				Term:       5,
				MatchIndex: 50,
				Success:    false,
			},
		},
		true,
		&pb.AppendEntriesResponse{
			Term:       5,
			MatchIndex: 50,
			Success:    false,
		},
	},
}

var qspecs = []struct {
	name string
	spec pb.QuorumSpec
}{
	{
		"QuorumSpec N3 Q=MajQ",
		&raft.QuorumSpec{N: 2, MajQ: 1, Q: 1},
	},
	{
		"QuorumSpec N3 Q=N",
		&raft.QuorumSpec{N: 2, MajQ: 1, Q: 2},
	},
}

func TestRequestVoteQF(t *testing.T) {
	qspec := qspecs[0]

	for _, test := range requestVoteQFTests {
		t.Run(qspec.name+"-"+test.name, func(t *testing.T) {
			reply, quorum := qspec.spec.RequestVoteQF(test.request, test.replies)

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
	qspec := qspecs[0]

	for _, test := range append(appendEntriesCommonQFTests, appendEntriesFastQFTests...) {
		t.Run(qspec.name+"-"+test.name, func(t *testing.T) {
			reply, quorum := qspec.spec.AppendEntriesQF(test.request, test.replies)

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
	qspec := qspecs[1]

	for _, test := range append(appendEntriesCommonQFTests, appendEntriesSlowQFTests...) {
		t.Run(qspec.name+"-"+test.name, func(t *testing.T) {
			reply, quorum := qspec.spec.AppendEntriesQF(test.request, test.replies)

			if quorum != test.quorum {
				t.Errorf("got %t, want %t", quorum, test.quorum)
			}

			if !reflect.DeepEqual(reply, test.reply) {
				t.Errorf("got %+v, want %+v", reply, test.reply)
			}
		})
	}
}
