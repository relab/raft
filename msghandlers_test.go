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

func (m *mockStorage) Set(key, value uint64) error {
	switch {
	case key == raft.KeyTerm:
		m.term = value
	case key == raft.KeyVotedFor:
		m.votedFor = value
	}
	return nil
}

func (m *mockStorage) Get(key uint64) (uint64, error) {
	if key == raft.KeyTerm {
		return m.term, nil
	}
	return m.votedFor, nil
}

func (m *mockStorage) StoreEntries(entries []*pb.Entry) error {
	m.log = append(m.log, entries...)
	return nil
}

func (m *mockStorage) GetEntry(index uint64) (*pb.Entry, error) {
	return m.log[int(index)], nil
}

func (m *mockStorage) GetEntries(from, to uint64) ([]*pb.Entry, error) {
	return m.log[int(from):int(to)], nil
}

func (m *mockStorage) RemoveEntriesFrom(index uint64) error {
	m.log = m.log[:index]
	return nil
}

func (m *mockStorage) NumEntries() uint64 {
	return uint64(len(m.log))
}

var log2 = []*pb.Entry{
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
}

func newTerm5() mockStorage {
	return mockStorage{term: 5}
}

func newTerm5log2() mockStorage {
	return mockStorage{term: 5, log: log2}
}

var term5 mockStorage
var term5log2 mockStorage

var handleRequestVoteRequestTests = []struct {
	name   string
	s      raft.Storage
	req    []*pb.RequestVoteRequest
	res    []*pb.RequestVoteResponse
	states []*mockStorage
}{
	{
		"reject lower term",
		&term5,
		[]*pb.RequestVoteRequest{&pb.RequestVoteRequest{CandidateID: 1, Term: 1}},
		[]*pb.RequestVoteResponse{&pb.RequestVoteResponse{Term: 5}},
		[]*mockStorage{&mockStorage{term: 5, votedFor: raft.None, log: nil}},
	},
	{
		"accept same term if not voted",
		&term5,
		[]*pb.RequestVoteRequest{&pb.RequestVoteRequest{CandidateID: 1, Term: 5}},
		[]*pb.RequestVoteResponse{&pb.RequestVoteResponse{Term: 5, VoteGranted: true}},
		[]*mockStorage{&mockStorage{term: 5, votedFor: 1, log: nil}},
	},
	{
		"accept one vote per term",
		&term5,
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
		[]*mockStorage{
			&mockStorage{term: 6, votedFor: 1, log: nil},
			&mockStorage{term: 6, votedFor: 1, log: nil},
			&mockStorage{term: 6, votedFor: 1, log: nil},
		},
	},
	{
		"accept higher terms",
		&term5,
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
		[]*mockStorage{
			&mockStorage{term: 5, votedFor: raft.None, log: nil},
			&mockStorage{term: 5, votedFor: 2, log: nil},
			&mockStorage{term: 6, votedFor: 3, log: nil},
		},
	},
	{
		"reject lower prevote term",
		&term5,
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 4, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5},
		},
		[]*mockStorage{&mockStorage{term: 5, votedFor: raft.None, log: nil}},
	},
	{
		"accept prevote in same term if not voted",
		&term5,
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 5, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
		},
		[]*mockStorage{&mockStorage{term: 5, votedFor: raft.None, log: nil}},
	},
	{
		"reject prevote in same term if voted",
		&term5,
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 5},
			&pb.RequestVoteRequest{CandidateID: 2, Term: 5, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 5},
		},
		[]*mockStorage{
			&mockStorage{term: 5, votedFor: 1, log: nil},
			&mockStorage{term: 5, votedFor: 1, log: nil},
		},
	},
	// TODO Don't grant pre-vote if heard from leader.
	{
		"accept prevote in higher term",
		&term5,
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 6, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 6, VoteGranted: true},
		},
		[]*mockStorage{&mockStorage{term: 5, votedFor: raft.None, log: nil}},
	},
	{
		// A pre-election is actually an election for the next term, so
		// a vote granted in an earlier term should not interfere.
		"accept prevote in higher term even if voted in current",
		&term5,
		[]*pb.RequestVoteRequest{
			&pb.RequestVoteRequest{CandidateID: 1, Term: 5},
			&pb.RequestVoteRequest{CandidateID: 2, Term: 6, PreVote: true},
		},
		[]*pb.RequestVoteResponse{
			&pb.RequestVoteResponse{Term: 5, VoteGranted: true},
			&pb.RequestVoteResponse{Term: 6, VoteGranted: true},
		},
		[]*mockStorage{
			&mockStorage{term: 5, votedFor: 1, log: nil},
			&mockStorage{term: 5, votedFor: 1, log: nil},
		},
	},
	{
		"reject log not up-to-date",
		&term5log2,
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
		[]*mockStorage{&mockStorage{term: 5, votedFor: raft.None, log: log2}},
	},
	{
		"reject log not up-to-date shorter log",
		&term5log2,
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
		[]*mockStorage{&mockStorage{term: 5, votedFor: raft.None, log: log2}},
	},
	{
		"reject log not up-to-date lower term",
		&term5log2,
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
		[]*mockStorage{&mockStorage{term: 5, votedFor: raft.None, log: log2}},
	},
	{
		"accpet log up-to-date",
		&term5log2,
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
		[]*mockStorage{&mockStorage{term: 5, votedFor: 1, log: log2}},
	},
	{
		"reject log up-to-date already voted",
		&term5log2,
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
		[]*mockStorage{
			&mockStorage{term: 5, votedFor: 1, log: log2},
			&mockStorage{term: 5, votedFor: 1, log: log2},
		},
	},
	{
		"accept log up-to-date already voted if higher term",
		&term5log2,
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
		[]*mockStorage{
			&mockStorage{term: 5, votedFor: 1, log: log2},
			&mockStorage{term: 6, votedFor: 2, log: log2},
		},
	},
}

func TestHandleRequestVoteRequest(t *testing.T) {
	for _, test := range handleRequestVoteRequestTests {
		t.Run(test.name, func(t *testing.T) {
			term5 = newTerm5()
			term5log2 = newTerm5log2()

			r := raft.NewReplica(&raft.Config{
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
