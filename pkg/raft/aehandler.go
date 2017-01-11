package raft

import (
	"bytes"
	"context"
	"fmt"
	"time"

	pb "github.com/relab/raft/pkg/raft/raftpb"
)

type appendEntriesHandler interface {
	sendRequest(*Replica)
	handleResponse(*Replica, *pb.AppendEntriesResponse)
}

type aeqrpc struct{}
type aenoqrpc struct{}

func (q *aeqrpc) sendRequest(r *Replica) {
	r.Lock()
	defer r.Unlock()

	if logLevel >= DEBUG {
		r.logger.log(fmt.Sprintf("Sending AppendEntries for term %d", r.persistent.currentTerm))
	}

	var buffer bytes.Buffer

LOOP:
	for i := r.maxAppendEntries; i > 0; i-- {
		select {
		case entry := <-r.queue:
			r.persistent.log = append(r.persistent.log, entry)

			buffer.WriteString(fmt.Sprintf(STORECOMMAND, r.persistent.currentTerm, entry.Data.ClientID, entry.Data.SequenceNumber, entry.Data.Command))
		default:
			break LOOP
		}
	}

	// Write to stable storage
	// TODO Assumes successful
	r.save(buffer.String())

	// #L1
	entries := []*pb.Entry{}

	nextIndex := 0

	// Always send entries for the most up-to-date client.
	for id := range r.nodes {
		next := r.nextIndex[id] - 1

		if next > nextIndex {
			nextIndex = next
		}
	}

	if len(r.persistent.log) > nextIndex {
		maxEntries := int(min(uint64(nextIndex+r.maxAppendEntries), uint64(len(r.persistent.log))))

		if !r.batch {
			maxEntries = nextIndex + 1
		}

		entries = r.persistent.log[nextIndex:maxEntries]
	}

	if logLevel >= DEBUG {
		r.logger.log(fmt.Sprintf("Sending %d entries", len(entries)))
	}

	req := &pb.AppendEntriesRequest{
		LeaderID:     r.id,
		Term:         r.persistent.currentTerm,
		PrevLogIndex: uint64(nextIndex),
		PrevLogTerm:  r.logTerm(nextIndex),
		CommitIndex:  r.commitIndex,
		Entries:      entries,
	}

	go func(req *pb.AppendEntriesRequest) {
		ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
		defer cancel()

		resp, err := r.conf.AppendEntries(ctx, req)

		if err != nil {
			r.logger.log(fmt.Sprintf("AppendEntries failed = %v", err))

			// We can not return if there is a response, i.e., there is replicas that needs updating.
			if resp.AppendEntriesResponse == nil {
				return
			}
		}

		r.aeHandler.handleResponse(r, resp.AppendEntriesResponse)
	}(req)

	r.heartbeat.Reset(r.heartbeatTimeout)
}

func (q *aenoqrpc) sendRequest(r *Replica) {
	r.Lock()
	defer r.Unlock()

	if logLevel >= DEBUG {
		r.logger.log(fmt.Sprintf("Sending AppendEntries for term %d", r.persistent.currentTerm))
	}

	var buffer bytes.Buffer

LOOP:
	for i := r.maxAppendEntries; i > 0; i-- {
		select {
		case entry := <-r.queue:
			r.persistent.log = append(r.persistent.log, entry)

			buffer.WriteString(fmt.Sprintf(STORECOMMAND, r.persistent.currentTerm, entry.Data.ClientID, entry.Data.SequenceNumber, entry.Data.Command))
		default:
			break LOOP
		}
	}

	// Write to stable storage
	// TODO Assumes successful
	r.save(buffer.String())

	// #L1
	for id, node := range r.nodes {
		entries := []*pb.Entry{}

		nextIndex := r.nextIndex[id] - 1

		if len(r.persistent.log) > nextIndex {
			maxEntries := int(min(uint64(nextIndex+r.maxAppendEntries), uint64(len(r.persistent.log))))

			if !r.batch {
				maxEntries = nextIndex + 1
			}

			entries = r.persistent.log[nextIndex:maxEntries]
		}

		if logLevel >= DEBUG {
			r.logger.to(id, fmt.Sprintf("Sending %d entries", len(entries)))
		}

		req := &pb.AppendEntriesRequest{
			LeaderID:     r.id,
			Term:         r.persistent.currentTerm,
			PrevLogIndex: uint64(nextIndex),
			PrevLogTerm:  r.logTerm(nextIndex),
			CommitIndex:  r.commitIndex,
			Entries:      entries,
		}

		go func(node *pb.Node, req *pb.AppendEntriesRequest) {
			ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
			defer cancel()

			resp, err := node.RaftClient.AppendEntries(ctx, req)

			if err != nil {
				r.logger.to(id, fmt.Sprintf("AppendEntries failed = %v", err))

				return
			}
			r.aeHandler.handleResponse(r, resp)
		}(node, req)
	}

	r.heartbeat.Reset(r.heartbeatTimeout)
}

func (q *aeqrpc) handleResponse(r *Replica, response *pb.AppendEntriesResponse) {
	r.Lock()
	defer func() {
		r.Unlock()
		r.advanceCommitIndex()
	}()

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if response.Term > r.persistent.currentTerm {
		r.becomeFollower(response.Term)

		return
	}

	// Ignore late response
	if response.Term < r.persistent.currentTerm {
		return
	}

	if r.state == Leader {
		if response.Success {
			for _, id := range response.FollowerID {
				r.matchIndex[id] = int(response.MatchIndex)
				r.nextIndex[id] = r.matchIndex[id] + 1
			}

			return
		}

		// If AppendEntries was not successful reset all.
		for id := range r.nodes {
			r.nextIndex[id] = int(response.MatchIndex)
		}
	}
}

func (q *aenoqrpc) handleResponse(r *Replica, response *pb.AppendEntriesResponse) {
	r.Lock()
	defer func() {
		r.Unlock()
		r.advanceCommitIndex()
	}()

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if response.Term > r.persistent.currentTerm {
		r.becomeFollower(response.Term)

		return
	}

	// Ignore late response
	if response.Term < r.persistent.currentTerm {
		return
	}

	if r.state == Leader {
		if response.Success {
			r.matchIndex[response.FollowerID[0]] = int(response.MatchIndex)
			r.nextIndex[response.FollowerID[0]] = r.matchIndex[response.FollowerID[0]] + 1

			return
		}

		r.nextIndex[response.FollowerID[0]] = int(max(1, response.MatchIndex))
	}
}
