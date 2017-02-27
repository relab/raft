package raftgorums_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/relab/raft/raftgorums"
	pb "github.com/relab/raft/raftgorums/raftpb"
	commonpb "github.com/relab/raft/raftpb"
)

var log2 = []*commonpb.Entry{
	&commonpb.Entry{
		Term: 4,
		Data: []byte("first"),
	},
	&commonpb.Entry{
		Term: 5,
		Data: []byte("second"),
	},
}

func newMemory(t uint64, l []*commonpb.Entry) *raftgorums.Memory {
	return raftgorums.NewMemory(map[uint64]uint64{
		raftgorums.KeyTerm:     t,
		raftgorums.KeyVotedFor: raftgorums.None,
	}, l)
}

var handleRequestVoteRequestTests = []struct {
	name   string
	s      raftgorums.Storage
	req    []*pb.RequestVoteRequest
	res    []*pb.RequestVoteResponse
	states []*raftgorums.Memory
}{
	{
		"reject lower term",
		newMemory(5, nil),
		[]*pb.RequestVoteRequest{&pb.RequestVoteRequest{CandidateID: 1, Term: 1}},
		[]*pb.RequestVoteResponse{&pb.RequestVoteResponse{Term: 5}},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: raftgorums.None,
			}, nil),
		},
	},
	{
		"accept same term if not voted",
		newMemory(5, nil),
		[]*pb.RequestVoteRequest{&pb.RequestVoteRequest{CandidateID: 1, Term: 5}},
		[]*pb.RequestVoteResponse{&pb.RequestVoteResponse{Term: 5, VoteGranted: true}},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: 1,
			}, nil),
		},
	},
	{
		"accept one vote per term",
		newMemory(5, nil),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 6},
			&pb.RequestVoteRequest{CandidateID: 2, Term: 6},
			&pb.RequestVoteRequest{CandidateID: 1, Term: 6},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 6, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 6, VoteGranted: false},
			// Multiple requests from the same candidate we voted
			// for (in the same term) must always return true. This
			// gives correct behavior even if the response is lost.
			&pb.RequestVoteResponse{Term: 6, VoteGranted: true},
		},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     6,
				raftgorums.KeyVotedFor: 1,
			}, nil),
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     6,
				raftgorums.KeyVotedFor: 1,
			}, nil),
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     6,
				raftgorums.KeyVotedFor: 1,
			}, nil),
		},
	},
	{
		"accept higher terms",
		newMemory(5, nil),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 4},
			&pb.RequestVoteRequest{CandidateID: 2, Term: 5},
			&pb.RequestVoteRequest{CandidateID: 3, Term: 6},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5},
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 6, VoteGranted: true},
		},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: raftgorums.None,
			}, nil),
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: 2,
			}, nil),
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     6,
				raftgorums.KeyVotedFor: 3,
			}, nil),
		},
	},
	{
		"reject lower prevote term",
		newMemory(5, nil),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 4, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5},
		},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: raftgorums.None,
			}, nil),
		},
	},
	{
		"accept prevote in same term if not voted",
		newMemory(5, nil),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 5, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
		},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: raftgorums.None,
			}, nil),
		},
	},
	{
		"reject prevote in same term if voted",
		newMemory(5, nil),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 5},
			&pb.RequestVoteRequest{CandidateID: 2, Term: 5, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 5},
		},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: 1,
			}, nil),
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: 1,
			}, nil),
		},
	},
	// TODO Don't grant pre-vote if heard from leader.
	{
		"accept prevote in higher term",
		newMemory(5, nil),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 6, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 6, VoteGranted: true},
		},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: raftgorums.None,
			}, nil),
		},
	},
	{
		// A pre-election is actually an election for the next term, so
		// a vote granted in an earlier term should not interfere.
		"accept prevote in higher term even if voted in current",
		newMemory(5, nil),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 5},
			&pb.RequestVoteRequest{CandidateID: 2, Term: 6, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 6, VoteGranted: true},
		},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: 1,
			}, nil),
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: 1,
			}, nil),
		},
	},
	{
		"reject log not up-to-date",
		newMemory(5, log2),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{
				CandidateID:  1,
				Term:         5,
				LastLogIndex: 0,
				LastLogTerm:  0,
			},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5},
		},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: raftgorums.None,
			}, log2),
		},
	},
	{
		"reject log not up-to-date shorter log",
		newMemory(5, log2),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{
				CandidateID:  1,
				Term:         5,
				LastLogIndex: 0,
				LastLogTerm:  5,
			},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5},
		},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: raftgorums.None,
			}, log2),
		},
	},
	{
		"reject log not up-to-date lower term",
		newMemory(5, log2),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{
				CandidateID:  1,
				Term:         5,
				LastLogIndex: 10,
				LastLogTerm:  4,
			},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5},
		},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: raftgorums.None,
			}, log2),
		},
	},
	{
		"accpet log up-to-date",
		newMemory(5, log2),
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{
				CandidateID:  1,
				Term:         5,
				LastLogIndex: 2,
				LastLogTerm:  5,
			},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
		},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: 1,
			}, log2),
		},
	},
	{
		"reject log up-to-date already voted",
		newMemory(5, log2),
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
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: 1,
			}, log2),
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: 1,
			}, log2),
		},
	},
	{
		"accept log up-to-date already voted if higher term",
		newMemory(5, log2),
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
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     5,
				raftgorums.KeyVotedFor: 1,
			}, log2),
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:     6,
				raftgorums.KeyVotedFor: 2,
			}, log2),
		},
	},
}

type noopMachine struct{}

func (n *noopMachine) Apply(*commonpb.Entry) interface{} {
	return nil
}

func TestHandleRequestVoteRequest(t *testing.T) {
	for _, test := range handleRequestVoteRequestTests {
		t.Run(test.name, func(t *testing.T) {
			r := raftgorums.NewRaft(&noopMachine{}, &raftgorums.Config{
				ElectionTimeout: time.Second,
				Storage:         test.s,
			})

			for i := 0; i < len(test.req); i++ {
				res := r.HandleRequestVoteRequest(test.req[i])

				if !reflect.DeepEqual(res, test.res[i]) {
					t.Errorf("got %+v, want %+v", res, test.res[i])
				}

				if !reflect.DeepEqual(test.s, test.states[i]) {
					t.Errorf("got %+v, want %+v", test.s, test.states[i])
				}
			}
		})
	}
}
