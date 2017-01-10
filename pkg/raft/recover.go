package raft

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	pb "github.com/relab/raft/pkg/raft/raftpb"
)

// How events are persisted to stable storage.
const (
	// TODO Should be in config. Default to tmp.
	STOREFILE     = "%d.storage"
	STORETERM     = "TERM,%d\n"
	STOREVOTEDFOR = "VOTED,%d\n"
	STORECOMMAND  = "%d,%d,%d,%s\n"
)

// TODO Should be application code.
func recoverFromStable(id uint64, recover bool) (*persistent, error) {
	p := &persistent{
		commands: make(map[uniqueCommand]*pb.ClientCommandRequest, BufferSize),
	}

	recoverFile := fmt.Sprintf(STOREFILE, id)

	if _, err := os.Stat(recoverFile); !os.IsNotExist(err) && recover {
		file, err := os.Open(recoverFile)

		if err != nil {
			return nil, err
		}

		defer file.Close()

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

				p.currentTerm = uint64(term)
			case "VOTED":
				votedFor, err := strconv.Atoi(split[1])

				if err != nil {
					return nil, err
				}

				p.votedFor = uint64(votedFor)
			default:
				var entry pb.Entry

				if len(split) != 4 {
					return nil, errCommaInCommand
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

				p.log = append(p.log, &entry)
				p.commands[uniqueCommand{entry.Data.ClientID, entry.Data.SequenceNumber}] = entry.Data
			}
		}

		if err := scanner.Err(); err != nil {
			return nil, err
		}

		// TODO Close if we were to implement graceful shutdown.
		p.recoverFile, err = os.OpenFile(recoverFile, os.O_APPEND|os.O_WRONLY, 0600)

		return p, err
	}

	// TODO Close if we were to implement graceful shutdown.
	var err error
	p.recoverFile, err = os.Create(recoverFile)

	return p, err
}
