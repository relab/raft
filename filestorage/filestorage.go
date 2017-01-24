package filestorage

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/relab/raft"
	pb "github.com/relab/raft/raftpb"
)

// How events are persisted to stable storage.
const (
	STOREFILE     = "%d.storage"
	STORETERM     = "TERM,%d\n"
	STOREVOTEDFOR = "VOTED,%d\n"
	STORECOMMAND  = "%d,%d,%d,%s\n"
)

// ErrCorruptedFile is returned if data cannot be retrieved from a file.
var ErrCorruptedFile = errors.New("File is corrupted.")

// FileStorage implements Storage using a file.
type FileStorage struct {
	file       *os.File
	persistent *raft.Persistent
}

// New returns a empty FileStorage.
func New(filename string) (*FileStorage, error) {
	file, err := os.Create(filename)

	if err != nil {
		return nil, err
	}

	return &FileStorage{
		file: file,
		persistent: &raft.Persistent{
			Commands: make(map[raft.UniqueCommand]*pb.ClientCommandRequest, raft.BufferSize),
		},
	}, nil
}

// FromFile return a FileStorage initialized from a file.
func FromFile(filename string) (*FileStorage, error) {
	_, err := os.Stat(filename)

	if os.IsNotExist(err) {
		return nil, err
	}

	file, err := os.Open(filename)

	if err != nil {
		return nil, err
	}

	// Load data
	p, err := loadFromFile(file)

	// Open append only.
	file, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)

	return &FileStorage{
		file:       file,
		persistent: p,
	}, nil
}

// Load implements the Storage interface. Can currently only be called once.
func (fs *FileStorage) Load() raft.Persistent {
	p := *fs.persistent
	fs.persistent = nil

	return p
}

// SaveState implements the Storage interface.
func (fs *FileStorage) SaveState(term uint64, votedFor uint64) error {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf(STORETERM, term))
	buffer.WriteString(fmt.Sprintf(STOREVOTEDFOR, votedFor))

	return fs.write(buffer.Bytes())
}

// SaveEntries implements the SaveEntries interface.
func (fs *FileStorage) SaveEntries(entries []*pb.Entry) error {
	var buffer bytes.Buffer

	for _, entry := range entries {
		buffer.WriteString(fmt.Sprintf(STORECOMMAND, entry.Term, entry.Data.ClientID, entry.Data.SequenceNumber, entry.Data.Command))
	}

	return fs.write(buffer.Bytes())
}

func (fs *FileStorage) write(buffer []byte) error {
	n, err := fs.file.Write(buffer)

	if err != nil {
		return err
	}

	if n < len(buffer) {
		return io.ErrShortWrite
	}

	return fs.file.Sync()
}

func loadFromFile(file *os.File) (*raft.Persistent, error) {
	p := &raft.Persistent{
		Commands: make(map[raft.UniqueCommand]*pb.ClientCommandRequest, raft.BufferSize),
	}

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		text := scanner.Text()
		split := strings.Split(text, ",")

		switch split[0] {
		case "TERM":
			term, err := strconv.Atoi(split[1])

			if err != nil {
				return nil, err
			}

			p.CurrentTerm = uint64(term)
		case "VOTED":
			votedFor, err := strconv.Atoi(split[1])

			if err != nil {
				return nil, err
			}

			p.VotedFor = uint64(votedFor)
		default:
			var entry pb.Entry

			if len(split) != 4 {
				return nil, ErrCorruptedFile
			}

			term, err := strconv.Atoi(split[0])

			if err != nil {
				return nil, err
			}

			clientID, err := strconv.Atoi(split[1])

			if err != nil {
				return nil, err
			}

			sequenceNumber, err := strconv.Atoi(split[2])

			if err != nil {
				return nil, err
			}

			command := split[3]

			entry.Term = uint64(term)
			entry.Data = &pb.ClientCommandRequest{ClientID: uint32(clientID), SequenceNumber: uint64(sequenceNumber), Command: command}

			p.Log = append(p.Log, &entry)
			p.Commands[raft.UniqueCommand{entry.Data.ClientID, entry.Data.SequenceNumber}] = entry.Data
		}
	}

	return p, scanner.Err()
}
