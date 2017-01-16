package raft

import (
	"bytes"
	"context"
	"fmt"
	"time"

	gorums "github.com/relab/raft/pkg/raft/gorumspb"
	pb "github.com/relab/raft/pkg/raft/raftpb"
)

type appendEntriesHandler interface {
	sendRequest(*Replica)
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

		r.handleAppendEntriesResponse(resp.AppendEntriesResponse)
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

		go func(node *gorums.Node, req *pb.AppendEntriesRequest) {
			ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
			defer cancel()

			resp, err := node.RaftClient.AppendEntries(ctx, req)

			if err != nil {
				r.logger.to(id, fmt.Sprintf("AppendEntries failed = %v", err))

				return
			}

			r.handleAppendEntriesResponse(resp)
		}(node, req)
	}

	r.heartbeat.Reset(r.heartbeatTimeout)
}
