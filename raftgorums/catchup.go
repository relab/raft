package raftgorums

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/relab/raft/commonpb"
	gorums "github.com/relab/raft/raftgorums/gorumspb"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

func (r *Raft) HandleInstallSnapshotRequest(snapshot *commonpb.Snapshot) (res *pb.InstallSnapshotResponse) {
	r.Lock()
	defer r.Unlock()

	res = &pb.InstallSnapshotResponse{
		Term: r.currentTerm,
	}

	// TODO Revise snapshot validation logic.

	// Don't install snapshot from outdated term.
	if snapshot.Term < r.currentTerm {
		return
	}

	// If last entry in snapshot exists in our log.
	switch {
	case snapshot.LastIncludedIndex == r.snapshotIndex:
		// Snapshot is already a prefix of our log, so
		// discard it.
		if snapshot.LastIncludedTerm == r.snapshotTerm {
			r.logger.log("received identical snapshot")
			return
		}

		r.logger.log(fmt.Sprintf("received snapshot with same index %d but different term %d != %d",
			snapshot.LastIncludedIndex, snapshot.LastIncludedTerm, r.snapshotTerm,
		))

	case snapshot.LastIncludedIndex < r.storage.NextIndex():
		entry, err := r.storage.GetEntry(snapshot.LastIncludedIndex)

		if err != nil {
			panic(fmt.Errorf("couldn't retrieve entry: %v", err))
		}

		// Snapshot is already a prefix of our log, so
		// discard it.
		if entry.Term == snapshot.LastIncludedTerm {
			r.logger.log("snapshot is already part of our log")
			return
		}
	}

	r.restoreCh <- snapshot
	return
}

func (r *Raft) HandleInstallSnapshotResponse(res *pb.InstallSnapshotResponse) bool {
	r.Lock()
	defer r.Unlock()

	if res.Term > r.currentTerm {
		r.becomeFollower(res.Term)

		return false
	}

	return true
}

func (r *Raft) HandleCatchMeUpRequest(req *pb.CatchMeUpRequest) {
	r.Lock()
	defer r.Unlock()

	if r.currentSnapshot != nil {
		// Update snapshot metadata before sending it.
		r.currentSnapshot.LeaderID = r.id
		r.currentSnapshot.Term = r.currentTerm
	}

	r.sreqout <- &snapshotRequest{
		followerID: req.FollowerID,
		snapshot:   r.currentSnapshot,
	}
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
