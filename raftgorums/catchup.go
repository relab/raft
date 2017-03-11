package raftgorums

import (
	"github.com/Sirupsen/logrus"
	"github.com/relab/raft/commonpb"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

func (r *Raft) HandleInstallSnapshotRequest(snapshot *commonpb.Snapshot) (res *pb.InstallSnapshotResponse) {
	r.Lock()
	defer r.Unlock()

	res = &pb.InstallSnapshotResponse{
		Term: r.currentTerm,
	}

	snapLogger := r.logger.WithFields(logrus.Fields{
		"currentterm":       r.currentTerm,
		"lastincludedindex": snapshot.LastIncludedIndex,
		"lastincludedterm":  snapshot.LastIncludedTerm,
		"snapshotIndex":     r.currentSnapshot.LastIncludedIndex,
		"snapshotTerm":      r.currentSnapshot.LastIncludedTerm,
	})
	snapLogger.Infoln("Received snapshot")

	// TODO Revise snapshot validation logic.

	// Don't install snapshot from outdated term.
	if snapshot.Term < r.currentTerm {
		return
	}

	// If last entry in snapshot exists in our log.
	switch {
	case snapshot.LastIncludedIndex == r.currentSnapshot.LastIncludedIndex:
		// Snapshot is already a prefix of our log, so
		// discard it.
		if snapshot.LastIncludedTerm == r.currentSnapshot.LastIncludedTerm {
			snapLogger.Infoln("Received identical snapshot")
			return
		}

		snapLogger.Warnln("Snapshot has same index but different term compared to ours")

	case snapshot.LastIncludedIndex < r.storage.NextIndex():
		entry := r.storage.GetEntry(snapshot.LastIncludedIndex)

		// Snapshot is already a prefix of our log, so
		// discard it.
		if entry.Term == snapshot.LastIncludedTerm {
			snapLogger.Warnln("Snapshot already part of our log")
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
