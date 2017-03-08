package raftgorums

import (
	"context"
	"log"
	"time"

	gorums "github.com/relab/raft/raftgorums/gorumspb"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

func (r *Raft) HandleInstallSnapshotResponse(res *pb.InstallSnapshotResponse) bool {
	r.Lock()
	defer r.Unlock()

	if res.Term > r.currentTerm {
		r.becomeFollower(res.Term)

		return false
	}

	return true
}

func (r *Raft) catchUp(conf *gorums.Configuration, nextIndex uint64, matchCh chan uint64) {
	defer close(matchCh)

	for {
		state := r.State()

		// If we are no longer the leader, stop catch-up.
		if state != Leader {
			return
		}

		r.Lock()
		entries := r.getNextEntries(nextIndex)
		request := r.getAppendEntriesRequest(nextIndex, entries)
		r.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
		res, err := conf.AppendEntries(ctx, request)
		cancel()

		log.Printf("Sending catch-up prevIndex:%d prevTerm:%d entries:%d",
			request.PrevLogIndex, request.PrevLogTerm, len(entries),
		)

		if err != nil {
			// TODO Better error message.
			log.Printf("Catch-up AppendEntries failed = %v\n", err)
			return
		}

		response := res.AppendEntriesResponse

		if response.Success {
			matchCh <- response.MatchIndex
			index := <-matchCh

			// If the indexes match, the follower has been added
			// back to the main configuration in time for the next
			// Appendentries.
			if response.MatchIndex == index {
				return
			}

			nextIndex = response.MatchIndex + 1

			continue
		}

		// If AppendEntries was not successful lower match index.
		nextIndex = max(1, response.MatchIndex)
	}
}
