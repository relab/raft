package raftgorums_test

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
	"github.com/relab/raft/raftgorums"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

func noop(index uint64, term uint64) *commonpb.Entry {
	return &commonpb.Entry{
		Index:     index,
		Term:      term,
		EntryType: commonpb.EntryNormal,
		Data:      raft.NOOP,
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
	maxae  uint64
	s      raft.Storage
	req    []*pb.AppendEntriesRequest
	res    []*pb.AppendEntriesResponse
	states []*raft.Memory
}{
	{
		"reject lower term",
		0,
		newMemory(5, nil),
		[]*pb.AppendEntriesRequest{{LeaderID: 1, Term: 1}},
		[]*pb.AppendEntriesResponse{{Term: 5}},
		[]*raft.Memory{
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      5,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 1,
			}, nil),
		},
	},
	{
		"successfully append entry",
		0,
		newMemory(5, log2()),
		[]*pb.AppendEntriesRequest{{
			LeaderID:     1,
			Term:         5,
			PrevLogIndex: 2,
			PrevLogTerm:  5,
			Entries: []*commonpb.Entry{
				noop(3, 5),
			},
		}},
		[]*pb.AppendEntriesResponse{{Term: 5, MatchIndex: 2, Success: true}},
		[]*raft.Memory{
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      5,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 4,
			}, logPlusEntry(log2(), noop(3, 5))),
		},
	},
	{
		"successfully overwrite entry",
		0,
		newMemory(5, logPlusEntry(log2(), noop(3, 5))),
		[]*pb.AppendEntriesRequest{{
			LeaderID:     1,
			Term:         6,
			PrevLogIndex: 2,
			PrevLogTerm:  5,
			Entries: []*commonpb.Entry{
				noop(3, 6),
			},
		}},
		[]*pb.AppendEntriesResponse{{Term: 6, MatchIndex: 3, Success: true}},
		[]*raft.Memory{
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 4,
			}, logPlusEntry(log2(), noop(3, 6))),
		},
	},
	{
		"successfully overwrite entries",
		0,
		newMemory(5, logPlusEntry(logPlusEntry(log2(), noop(3, 5)), noop(4, 5))),
		[]*pb.AppendEntriesRequest{{
			LeaderID:     1,
			Term:         6,
			PrevLogIndex: 2,
			PrevLogTerm:  5,
			Entries: []*commonpb.Entry{
				noop(3, 6), noop(4, 6),
			},
		}},
		[]*pb.AppendEntriesResponse{{Term: 6, MatchIndex: 4, Success: true}},
		[]*raft.Memory{
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 5,
			}, logPlusEntry(logPlusEntry(log2(), noop(3, 6)), noop(4, 6))),
		},
	},
	{
		"successful on already committed but ignore entries",
		0,
		newMemory(5, log2()),
		[]*pb.AppendEntriesRequest{
			{
				LeaderID:     1,
				Term:         5,
				PrevLogIndex: 2,
				PrevLogTerm:  5,
				CommitIndex:  3,
				Entries: []*commonpb.Entry{
					noop(3, 5),
				},
			},
			{
				LeaderID:     1,
				Term:         5,
				PrevLogIndex: 3,
				PrevLogTerm:  5,
				CommitIndex:  4,
				Entries: []*commonpb.Entry{
					noop(4, 5),
				},
			},
			{
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
			{Term: 5, MatchIndex: 2, Success: true},
			{Term: 5, MatchIndex: 3, Success: true},
			{Term: 5, MatchIndex: 4, Success: true},
		},
		[]*raft.Memory{
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      5,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 4,
			}, logPlusEntry(log2(), noop(3, 5))),
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      5,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 5,
			}, logPlusEntry(logPlusEntry(log2(), noop(3, 5)), noop(4, 5))),
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      5,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 5,
			}, logPlusEntry(logPlusEntry(log2(), noop(3, 5)), noop(4, 5))),
		},
	},
	{
		"raft paper fig 7 follower a",
		0,
		raft.NewMemory(map[uint64]uint64{
			raft.KeyTerm:      6,
			raft.KeyVotedFor:  3,
			raft.KeyNextIndex: 10,
		}, map[uint64]*commonpb.Entry{
			1: noop(1, 1),
			2: noop(2, 1),
			3: noop(3, 1),
			4: noop(4, 4),
			5: noop(5, 4),
			6: noop(6, 5),
			7: noop(7, 5),
			8: noop(8, 6),
			9: noop(9, 6),
		}),
		[]*pb.AppendEntriesRequest{
			{
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 10,
				PrevLogTerm:  6,
				Entries: []*commonpb.Entry{
					noop(11, 6),
				},
			},
			{
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 9,
				PrevLogTerm:  6,
				Entries: []*commonpb.Entry{
					noop(10, 6),
					noop(11, 6),
				},
			},
		},
		[]*pb.AppendEntriesResponse{
			{Term: 6, MatchIndex: 9},
			{Term: 6, MatchIndex: 9, Success: true},
		},
		[]*raft.Memory{
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  3,
				raft.KeyNextIndex: 10,
			}, map[uint64]*commonpb.Entry{
				1: noop(1, 1),
				2: noop(2, 1),
				3: noop(3, 1),
				4: noop(4, 4),
				5: noop(5, 4),
				6: noop(6, 5),
				7: noop(7, 5),
				8: noop(8, 6),
				9: noop(9, 6),
			}),
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  3,
				raft.KeyNextIndex: 12,
			}, map[uint64]*commonpb.Entry{
				1:  noop(1, 1),
				2:  noop(2, 1),
				3:  noop(3, 1),
				4:  noop(4, 4),
				5:  noop(5, 4),
				6:  noop(6, 5),
				7:  noop(7, 5),
				8:  noop(8, 6),
				9:  noop(9, 6),
				10: noop(10, 6),
				11: noop(11, 6),
			}),
		},
	},
	{
		"raft paper fig 7 follower b",
		0,
		raft.NewMemory(map[uint64]uint64{
			raft.KeyTerm:      4,
			raft.KeyVotedFor:  3,
			raft.KeyNextIndex: 5,
		}, map[uint64]*commonpb.Entry{
			1: noop(1, 1),
			2: noop(2, 1),
			3: noop(3, 1),
			4: noop(4, 4),
		}),
		[]*pb.AppendEntriesRequest{
			{
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 10,
				PrevLogTerm:  6,
				Entries: []*commonpb.Entry{
					noop(11, 6),
				},
			},
			{
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 4,
				PrevLogTerm:  4,
				Entries: []*commonpb.Entry{
					noop(5, 4),
					noop(6, 5),
					noop(7, 5),
					noop(8, 6),
					noop(9, 6),
					noop(10, 6),
					noop(11, 6),
				},
			},
		},
		[]*pb.AppendEntriesResponse{
			{Term: 6, MatchIndex: 4},
			{Term: 6, MatchIndex: 4, Success: true},
		},
		[]*raft.Memory{
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 5,
			}, map[uint64]*commonpb.Entry{
				1: noop(1, 1),
				2: noop(2, 1),
				3: noop(3, 1),
				4: noop(4, 4),
			}),
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 12,
			}, map[uint64]*commonpb.Entry{
				1:  noop(1, 1),
				2:  noop(2, 1),
				3:  noop(3, 1),
				4:  noop(4, 4),
				5:  noop(5, 4),
				6:  noop(6, 5),
				7:  noop(7, 5),
				8:  noop(8, 6),
				9:  noop(9, 6),
				10: noop(10, 6),
				11: noop(11, 6),
			}),
		},
	},
	{
		"raft paper fig 7 follower c",
		0,
		raft.NewMemory(map[uint64]uint64{
			raft.KeyTerm:      6,
			raft.KeyVotedFor:  3,
			raft.KeyNextIndex: 12,
		}, map[uint64]*commonpb.Entry{
			1:  noop(1, 1),
			2:  noop(2, 1),
			3:  noop(3, 1),
			4:  noop(4, 4),
			5:  noop(5, 4),
			6:  noop(6, 5),
			7:  noop(7, 5),
			8:  noop(8, 6),
			9:  noop(9, 6),
			10: noop(10, 6),
			11: noop(11, 6),
		}),
		[]*pb.AppendEntriesRequest{
			{
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 10,
				PrevLogTerm:  6,
				Entries: []*commonpb.Entry{
					noop(11, 6),
				},
			},
		},
		[]*pb.AppendEntriesResponse{
			{Term: 6, MatchIndex: 11, Success: true},
		},
		[]*raft.Memory{
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  3,
				raft.KeyNextIndex: 12,
			}, map[uint64]*commonpb.Entry{
				1:  noop(1, 1),
				2:  noop(2, 1),
				3:  noop(3, 1),
				4:  noop(4, 4),
				5:  noop(5, 4),
				6:  noop(6, 5),
				7:  noop(7, 5),
				8:  noop(8, 6),
				9:  noop(9, 6),
				10: noop(10, 6),
				11: noop(11, 6),
			}),
		},
	},
	{
		"raft paper fig 7 follower d",
		0,
		raft.NewMemory(map[uint64]uint64{
			raft.KeyTerm:      7,
			raft.KeyVotedFor:  3,
			raft.KeyNextIndex: 13,
		}, map[uint64]*commonpb.Entry{
			1:  noop(1, 1),
			2:  noop(2, 1),
			3:  noop(3, 1),
			4:  noop(4, 4),
			5:  noop(5, 4),
			6:  noop(6, 5),
			7:  noop(7, 5),
			8:  noop(8, 6),
			9:  noop(9, 6),
			10: noop(10, 6),
			11: noop(11, 7),
			12: noop(12, 7),
		}),
		[]*pb.AppendEntriesRequest{
			{
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 10,
				PrevLogTerm:  6,
				Entries: []*commonpb.Entry{
					noop(11, 6),
				},
			},
		},
		[]*pb.AppendEntriesResponse{
			{Term: 7, MatchIndex: 12},
		},
		[]*raft.Memory{
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      7,
				raft.KeyVotedFor:  3,
				raft.KeyNextIndex: 13,
			}, map[uint64]*commonpb.Entry{
				1:  noop(1, 1),
				2:  noop(2, 1),
				3:  noop(3, 1),
				4:  noop(4, 4),
				5:  noop(5, 4),
				6:  noop(6, 5),
				7:  noop(7, 5),
				8:  noop(8, 6),
				9:  noop(9, 6),
				10: noop(10, 6),
				11: noop(11, 7),
				12: noop(12, 7),
			}),
		},
	},
	{
		"raft paper fig 7 follower e",
		0,
		raft.NewMemory(map[uint64]uint64{
			raft.KeyTerm:      4,
			raft.KeyVotedFor:  3,
			raft.KeyNextIndex: 8,
		}, map[uint64]*commonpb.Entry{
			1: noop(1, 1),
			2: noop(2, 1),
			3: noop(3, 1),
			4: noop(4, 4),
			5: noop(5, 4),
			6: noop(6, 4),
			7: noop(7, 4),
		}),
		[]*pb.AppendEntriesRequest{
			{
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 10,
				PrevLogTerm:  6,
				Entries: []*commonpb.Entry{
					noop(11, 6),
				},
			},
			{
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 7,
				PrevLogTerm:  5,
				Entries: []*commonpb.Entry{
					noop(8, 6),
					noop(9, 6),
					noop(10, 6),
					noop(11, 6),
				},
			},
		},
		[]*pb.AppendEntriesResponse{
			{Term: 6, MatchIndex: 7},
			{Term: 6, MatchIndex: 7},
		},
		[]*raft.Memory{
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 8,
			}, map[uint64]*commonpb.Entry{
				1: noop(1, 1),
				2: noop(2, 1),
				3: noop(3, 1),
				4: noop(4, 4),
				5: noop(5, 4),
				6: noop(6, 4),
				7: noop(7, 4),
			}),
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 8,
			}, map[uint64]*commonpb.Entry{
				1: noop(1, 1),
				2: noop(2, 1),
				3: noop(3, 1),
				4: noop(4, 4),
				5: noop(5, 4),
				6: noop(6, 4),
				7: noop(7, 4),
			}),
		},
	},
	{
		"out of order aereq without buffer",
		0,
		raft.NewMemory(map[uint64]uint64{
			raft.KeyTerm:      4,
			raft.KeyVotedFor:  3,
			raft.KeyNextIndex: 8,
		}, map[uint64]*commonpb.Entry{
			1: noop(1, 1),
			2: noop(2, 1),
			3: noop(3, 1),
			4: noop(4, 4),
			5: noop(5, 4),
			6: noop(6, 4),
			7: noop(7, 4),
		}),
		[]*pb.AppendEntriesRequest{
			{
				//this one is out of order, rejected but stored
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 8,
				PrevLogTerm:  6,
				Entries: []*commonpb.Entry{
					noop(9, 6),
				},
			},
			{
				//this one is in order, should be applied
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 7,
				PrevLogTerm:  4,
				Entries: []*commonpb.Entry{
					noop(8, 6),
				},
			},
			{
				//this one is in order, if the stored one was applied
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 9,
				PrevLogTerm:  6,
				Entries: []*commonpb.Entry{
					noop(10, 6),
				},
			},
		},
		[]*pb.AppendEntriesResponse{
			{Term: 6, MatchIndex: 7},
			{Term: 6, MatchIndex: 7, Success: true},
			{Term: 6, MatchIndex: 8, Success: false},
		},
		[]*raft.Memory{
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 8,
			}, map[uint64]*commonpb.Entry{
				1: noop(1, 1),
				2: noop(2, 1),
				3: noop(3, 1),
				4: noop(4, 4),
				5: noop(5, 4),
				6: noop(6, 4),
				7: noop(7, 4),
			}),
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 9,
			}, map[uint64]*commonpb.Entry{
				1: noop(1, 1),
				2: noop(2, 1),
				3: noop(3, 1),
				4: noop(4, 4),
				5: noop(5, 4),
				6: noop(6, 4),
				7: noop(7, 4),
				8: noop(8, 6),
			}),
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 9,
			}, map[uint64]*commonpb.Entry{
				1: noop(1, 1),
				2: noop(2, 1),
				3: noop(3, 1),
				4: noop(4, 4),
				5: noop(5, 4),
				6: noop(6, 4),
				7: noop(7, 4),
				8: noop(8, 6),
			}),
		},
	},
	{
		"out of order aereq with buffering",
		1,
		raft.NewMemory(map[uint64]uint64{
			raft.KeyTerm:      4,
			raft.KeyVotedFor:  3,
			raft.KeyNextIndex: 8,
		}, map[uint64]*commonpb.Entry{
			1: noop(1, 1),
			2: noop(2, 1),
			3: noop(3, 1),
			4: noop(4, 4),
			5: noop(5, 4),
			6: noop(6, 4),
			7: noop(7, 4),
		}),
		[]*pb.AppendEntriesRequest{
			{
				//this one is out of order, rejected but stored
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 8,
				PrevLogTerm:  6,
				Entries: []*commonpb.Entry{
					noop(9, 6),
				},
			},
			{
				//this one is in order, should be applied
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 7,
				PrevLogTerm:  4,
				Entries: []*commonpb.Entry{
					noop(8, 6),
				},
			},
			{
				//this one is in order, if the stored one was applied
				LeaderID:     1,
				Term:         6,
				PrevLogIndex: 9,
				PrevLogTerm:  6,
				Entries: []*commonpb.Entry{
					noop(10, 6),
				},
			},
		},
		[]*pb.AppendEntriesResponse{
			{Term: 6, MatchIndex: 7},
			{Term: 6, MatchIndex: 7, Success: true},
			{Term: 6, MatchIndex: 9, Success: true},
		},
		[]*raft.Memory{
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 8,
			}, map[uint64]*commonpb.Entry{
				1: noop(1, 1),
				2: noop(2, 1),
				3: noop(3, 1),
				4: noop(4, 4),
				5: noop(5, 4),
				6: noop(6, 4),
				7: noop(7, 4),
			}),
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 10,
			}, map[uint64]*commonpb.Entry{
				1: noop(1, 1),
				2: noop(2, 1),
				3: noop(3, 1),
				4: noop(4, 4),
				5: noop(5, 4),
				6: noop(6, 4),
				7: noop(7, 4),
				8: noop(8, 6),
				9: noop(9, 6),
			}),
			raft.NewMemory(map[uint64]uint64{
				raft.KeyTerm:      6,
				raft.KeyVotedFor:  raftgorums.None,
				raft.KeyNextIndex: 11,
			}, map[uint64]*commonpb.Entry{
				1:  noop(1, 1),
				2:  noop(2, 1),
				3:  noop(3, 1),
				4:  noop(4, 4),
				5:  noop(5, 4),
				6:  noop(6, 4),
				7:  noop(7, 4),
				8:  noop(8, 6),
				9:  noop(9, 6),
				10: noop(10, 6),
			}),
		},
	},
}

func TestHandleAppendEntriesRequest(t *testing.T) {
	l := logrus.New()
	l.Out = ioutil.Discard

	for _, test := range handleAppendEntriesRequestTests {
		t.Run(test.name, func(t *testing.T) {
			fmt.Println("test ", test.name)
			r := raftgorums.NewRaft(&noopMachine{}, &raftgorums.Config{
				ID:               1,
				ElectionTimeout:  time.Second,
				Storage:          test.s,
				Logger:           l,
				MaxAEBuffer:      test.maxae,
				MaxMissingCommit: 11,
			}, raft.NewLatency(), raft.NewEvent(), make(chan struct{}))

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
