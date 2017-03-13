package raftgorums_test

import (
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/relab/raft/commonpb"
	"github.com/relab/raft/raftgorums"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

func noop(index uint64, term uint64) *commonpb.Entry {
	return &commonpb.Entry{
		Index:     index,
		Term:      term,
		EntryType: commonpb.EntryInternal,
		Data:      []byte("noop"),
	}
}

func logPlusEntry(l map[uint64]*commonpb.Entry, entry *commonpb.Entry) map[uint64]*commonpb.Entry {
	nl := make(map[uint64]*commonpb.Entry)

	for k, v := range l {
		nl[k] = v
	}

	nl[entry.Index] = entry

	return nl
}

var handleAppendEntriesRequestTests = []struct {
	name   string
	s      raftgorums.Storage
	req    []*pb.AppendEntriesRequest
	res    []*pb.AppendEntriesResponse
	states []*raftgorums.Memory
}{
	{
		"reject lower term",
		newMemory(5, nil),
		[]*pb.AppendEntriesRequest{&pb.AppendEntriesRequest{LeaderID: 1, Term: 1}},
		[]*pb.AppendEntriesResponse{&pb.AppendEntriesResponse{Term: 5}},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:      5,
				raftgorums.KeyVotedFor:  raftgorums.None,
				raftgorums.KeyNextIndex: 1,
			}, nil),
		},
	},
	{
		"successfully append entry",
		newMemory(5, log2()),
		[]*pb.AppendEntriesRequest{&pb.AppendEntriesRequest{
			LeaderID:     1,
			Term:         5,
			PrevLogIndex: 2,
			PrevLogTerm:  5,
			Entries: []*commonpb.Entry{
				noop(3, 5),
			},
		}},
		[]*pb.AppendEntriesResponse{&pb.AppendEntriesResponse{Term: 5, MatchIndex: 3, Success: true}},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:      5,
				raftgorums.KeyVotedFor:  raftgorums.None,
				raftgorums.KeyNextIndex: 4,
			}, logPlusEntry(log2(), noop(3, 5))),
		},
	},
	{
		"successfully overwrite entry",
		newMemory(5, logPlusEntry(log2(), noop(3, 5))),
		[]*pb.AppendEntriesRequest{&pb.AppendEntriesRequest{
			LeaderID:     1,
			Term:         6,
			PrevLogIndex: 2,
			PrevLogTerm:  5,
			Entries: []*commonpb.Entry{
				noop(3, 6),
			},
		}},
		[]*pb.AppendEntriesResponse{&pb.AppendEntriesResponse{Term: 6, MatchIndex: 3, Success: true}},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:      6,
				raftgorums.KeyVotedFor:  raftgorums.None,
				raftgorums.KeyNextIndex: 4,
			}, logPlusEntry(log2(), noop(3, 6))),
		},
	},
	{
		"successfully overwrite entries",
		newMemory(5, logPlusEntry(logPlusEntry(log2(), noop(3, 5)), noop(4, 5))),
		[]*pb.AppendEntriesRequest{&pb.AppendEntriesRequest{
			LeaderID:     1,
			Term:         6,
			PrevLogIndex: 2,
			PrevLogTerm:  5,
			Entries: []*commonpb.Entry{
				noop(3, 6), noop(4, 6),
			},
		}},
		[]*pb.AppendEntriesResponse{&pb.AppendEntriesResponse{Term: 6, MatchIndex: 4, Success: true}},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:      6,
				raftgorums.KeyVotedFor:  raftgorums.None,
				raftgorums.KeyNextIndex: 5,
			}, logPlusEntry(logPlusEntry(log2(), noop(3, 6)), noop(4, 6))),
		},
	},
	{
		"successful on already committed but ignore entries",
		newMemory(5, log2()),
		[]*pb.AppendEntriesRequest{
			&pb.AppendEntriesRequest{
				LeaderID:     1,
				Term:         5,
				PrevLogIndex: 2,
				PrevLogTerm:  5,
				CommitIndex:  3,
				Entries: []*commonpb.Entry{
					noop(3, 5),
				},
			},
			&pb.AppendEntriesRequest{
				LeaderID:     1,
				Term:         5,
				PrevLogIndex: 3,
				PrevLogTerm:  5,
				CommitIndex:  4,
				Entries: []*commonpb.Entry{
					noop(4, 5),
				},
			},
			&pb.AppendEntriesRequest{
				LeaderID:     1,
				Term:         5,
				PrevLogIndex: 2,
				PrevLogTerm:  5,
				CommitIndex:  3,
				Entries: []*commonpb.Entry{
					noop(3, 5),
				},
			},
		},
		[]*pb.AppendEntriesResponse{
			&pb.AppendEntriesResponse{Term: 5, MatchIndex: 3, Success: true},
			&pb.AppendEntriesResponse{Term: 5, MatchIndex: 4, Success: true},
			&pb.AppendEntriesResponse{Term: 5, MatchIndex: 4, Success: true},
		},
		[]*raftgorums.Memory{
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:      5,
				raftgorums.KeyVotedFor:  raftgorums.None,
				raftgorums.KeyNextIndex: 4,
			}, logPlusEntry(log2(), noop(3, 5))),
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:      5,
				raftgorums.KeyVotedFor:  raftgorums.None,
				raftgorums.KeyNextIndex: 5,
			}, logPlusEntry(logPlusEntry(log2(), noop(3, 5)), noop(4, 5))),
			raftgorums.NewMemory(map[uint64]uint64{
				raftgorums.KeyTerm:      5,
				raftgorums.KeyVotedFor:  raftgorums.None,
				raftgorums.KeyNextIndex: 5,
			}, logPlusEntry(logPlusEntry(log2(), noop(3, 5)), noop(4, 5))),
		},
	},
}

func TestHandleAppendEntriesRequest(t *testing.T) {
	l := logrus.New()
	l.Out = ioutil.Discard

	for _, test := range handleAppendEntriesRequestTests {
		t.Run(test.name, func(t *testing.T) {
			r := raftgorums.NewRaft(&noopMachine{}, &raftgorums.Config{
				ElectionTimeout: time.Second,
				Storage:         test.s,
				Logger:          l,
			})

			for i := 0; i < len(test.req); i++ {
				res := r.HandleAppendEntriesRequest(test.req[i])

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
