package raft_test

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/relab/raft"
	pb "github.com/relab/raft/raftpb"
)

const testFilename = "test.storage"

var empty = raft.Persistent{
	Commands: make(map[raft.UniqueCommand]*pb.ClientCommandRequest, raft.BufferSize),
}

var fileStorageTests = []struct {
	name     string
	term     uint64
	votedFor uint64
	fromFile bool
	entries  []*pb.Entry
	expected raft.Persistent
}{
	{
		name:     "empty",
		expected: empty,
	},
	{
		name:     "term and votedfor",
		term:     5,
		votedFor: 1,
		fromFile: true,
		expected: raft.Persistent{
			CurrentTerm: 5,
			VotedFor:    1,
			Commands:    make(map[raft.UniqueCommand]*pb.ClientCommandRequest, raft.BufferSize),
		},
	},
	{
		name:     "entries",
		entries:  []*pb.Entry{&pb.Entry{Data: &pb.ClientCommandRequest{}}},
		fromFile: true,
		expected: raft.Persistent{
			Log: []*pb.Entry{&pb.Entry{Data: &pb.ClientCommandRequest{}}},
			Commands: map[raft.UniqueCommand]*pb.ClientCommandRequest{
				raft.UniqueCommand{}: &pb.ClientCommandRequest{},
			},
		},
	},
	{
		name:     "all",
		term:     5,
		votedFor: 1,
		entries:  []*pb.Entry{&pb.Entry{Data: &pb.ClientCommandRequest{}}},
		fromFile: true,
		expected: raft.Persistent{
			CurrentTerm: 5,
			VotedFor:    1,
			Log:         []*pb.Entry{&pb.Entry{Data: &pb.ClientCommandRequest{}}},
			Commands: map[raft.UniqueCommand]*pb.ClientCommandRequest{
				raft.UniqueCommand{}: &pb.ClientCommandRequest{},
			},
		},
	},
}

func TestFileStorage(t *testing.T) {
	for _, test := range fileStorageTests {
		t.Run(test.name, func(t *testing.T) {
			fs, err := raft.New(testFilename)

			if err != nil {
				t.Error(err)
			}

			got := fs.Load()

			if !reflect.DeepEqual(got, empty) {
				t.Errorf("got %+v, want %+v", got, empty)
			}

			if test.fromFile {
				if test.term > 0 || test.votedFor > 0 {
					err := fs.SaveState(test.term, test.votedFor)

					if err != nil {
						t.Error(err)
					}
				}

				if len(test.entries) > 0 {
					err := fs.SaveEntries(test.entries)

					if err != nil {
						t.Error(err)
					}
				}

				fs, err := raft.FromFile(testFilename)

				if err != nil {
					t.Error(err)
				}

				got := fs.Load()

				if !reflect.DeepEqual(got, test.expected) {
					t.Errorf("got %+v, want %+v", got, test.expected)
				}
			}

			err = os.Remove(testFilename)

			if err != nil {
				t.Error("Error removing:", testFilename, "=>", err)
			}
		})
	}
}

func BenchmarkFileStorage(b *testing.B) {
	fs, err := raft.New(testFilename)

	if err != nil {
		b.Error(err)
	}

	var entries []*pb.Entry
	command := strings.Repeat("x", 16)

	for i := 0; i < 5000; i++ {
		entries = append(entries, &pb.Entry{
			Term: 99999,
			Data: &pb.ClientCommandRequest{
				ClientID:       88888,
				SequenceNumber: 7777,
				Command:        command,
			},
		})
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := fs.SaveEntries(entries)

		if err != nil {
			b.Error(err)
		}
	}

	b.StopTimer()

	err = os.Remove(testFilename)

	if err != nil {
		b.Error("Error removing:", testFilename, "=>", err)
	}
}
