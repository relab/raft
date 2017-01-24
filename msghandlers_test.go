package raft_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/relab/raft"
	pb "github.com/relab/raft/raftpb"
)

type mockStorage struct {
	term     uint64
	votedFor uint64
	log      []*pb.Entry
}

func (s *mockStorage) SaveState(uint64, uint64) error {
	return nil
}

func (s *mockStorage) SaveEntries([]*pb.Entry) error {
	return nil
}

func (s *mockStorage) Load() raft.Persistent {
	commands := make(map[raft.UniqueCommand]*pb.ClientCommandRequest)

	for _, entry := range s.log {
		commands[raft.UniqueCommand{
			ClientID:       entry.Data.ClientID,
			SequenceNumber: entry.Data.SequenceNumber,
		}] = entry.Data
	}

	return raft.Persistent{
		CurrentTerm: s.term,
		VotedFor:    s.votedFor,
		Log:         s.log,
		Commands:    commands,
	}
}

var term5 = &raft.Config{
	ElectionTimeout: time.Second,
	Storage:         &mockStorage{term: 5},
}

var term5log2 = &raft.Config{
	ElectionTimeout: time.Second,
	Storage: &mockStorage{
		term: 5,
		log: []*pb.Entry{
			&pb.Entry{
				Term: 4,
				Data: &pb.ClientCommandRequest{
					ClientID:       123,
					SequenceNumber: 456,
					Command:        "first",
				},
			},
			&pb.Entry{
				Term: 5,
				Data: &pb.ClientCommandRequest{
					ClientID:       123,
					SequenceNumber: 457,
					Command:        "second",
				},
			},
		},
	},
}

var handleRequestVoteRequestTests = []struct {
	name string
	r    *raft.Replica
	req  []*pb.RequestVoteRequest
	res  []*pb.RequestVoteResponse
}{
	{
		"lower term",
		raft.NewReplica(term5),
		[]*pb.RequestVoteRequest{&pb.RequestVoteRequest{Term: 1}},
		[]*pb.RequestVoteResponse{&pb.RequestVoteResponse{Term: 5}},
	},
	{
		"high then low term",
		raft.NewReplica(term5),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{Term: 7},
			&pb.RequestVoteRequest{Term: 1},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 7, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 7},
		},
	},
	{
		"increasing terms",
		raft.NewReplica(term5),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{Term: 4},
			&pb.RequestVoteRequest{Term: 5},
			&pb.RequestVoteRequest{Term: 6},
			&pb.RequestVoteRequest{Term: 7},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5},
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 6, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 7, VoteGranted: true},
		},
	},
	{
		"already voted",
		raft.NewReplica(term5),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 5},
			&pb.RequestVoteRequest{CandidateID: 2, Term: 5},
			&pb.RequestVoteRequest{CandidateID: 1, Term: 5},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 5, VoteGranted: false},
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
		},
	},
	{
		"pre-vote lower term",
		raft.NewReplica(term5),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{Term: 4, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5},
		},
	},
	{
		"pre-vote same term",
		raft.NewReplica(term5),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{Term: 5, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5},
		},
	},
	{
		"pre-vote higher term",
		raft.NewReplica(term5),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 6, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 6, VoteGranted: true},
		},
	},
	{
		// A pre-election is actually an election for the next term, so
		// a vote granted in an earlier term should not interfere.
		"pre-vote already voted",
		raft.NewReplica(term5),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 5},
			&pb.RequestVoteRequest{CandidateID: 2, Term: 6, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 6, VoteGranted: true},
		},
	},
	{
		"log not up-to-date",
		raft.NewReplica(term5log2),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{
				Term:         5,
				LastLogIndex: 0,
				LastLogTerm:  0,
			},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5},
		},
	},
	{
		"log not up-to-date shorter log",
		raft.NewReplica(term5log2),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{
				Term:         5,
				LastLogIndex: 0,
				LastLogTerm:  5,
			},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5},
		},
	},
	{
		"log not up-to-date lower term",
		raft.NewReplica(term5log2),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{
				Term:         5,
				LastLogIndex: 10,
				LastLogTerm:  4,
			},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5},
		},
	},
	{
		"log up-to-date",
		raft.NewReplica(term5log2),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{
				Term:         5,
				LastLogIndex: 2,
				LastLogTerm:  5,
			},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
		},
	},
	{
		"log up-to-date already voted",
		raft.NewReplica(term5log2),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{
				CandidateID:  1,
				Term:         5,
				LastLogIndex: 2,
				LastLogTerm:  5,
			},
			&pb.RequestVoteRequest{
				CandidateID:  2,
				Term:         5,
				LastLogIndex: 15,
				LastLogTerm:  5,
			},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 5},
		},
	},
	{
		"log up-to-date already voted higher term",
		raft.NewReplica(term5log2),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{
				CandidateID:  1,
				Term:         5,
				LastLogIndex: 2,
				LastLogTerm:  5,
			},
			&pb.RequestVoteRequest{
				CandidateID:  2,
				Term:         6,
				LastLogIndex: 2,
				LastLogTerm:  5,
			},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 6, VoteGranted: true},
		},
	},
}

func TestHandleRequestVoteRequest(t *testing.T) {
	for _, test := range handleRequestVoteRequestTests {
		t.Run(test.name, func(t *testing.T) {
			for i := 0; i < len(test.req); i++ {
				res := test.r.HandleRequestVoteRequest(test.req[i])

				if !reflect.DeepEqual(res, test.res[i]) {
					t.Errorf("got %+v, want %+v", res, test.res[i])
				}
			}
		})
	}
}
