package raftgorums

import (
	"golang.org/x/net/context"

	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
)

// ProposeConf implements raft.Raft.
func (r *Raft) ProposeConf(ctx context.Context, confChange *commonpb.ConfChangeRequest) (raft.Future, error) {
	// TODO Tag conf. change with the only configuration it can applied on top of.
	// confChange.PrevConf = r.latestConf

	cmd, err := confChange.Marshal()

	if err != nil {
		return nil, err
	}

	future, err := r.cmdToFuture(cmd, commonpb.EntryConfChange)

	if err != nil {
		return nil, err
	}

	go r.replicate(confChange.ServerID, future)

	return future, nil
}

// ProposeCmd implements raft.Raft.
func (r *Raft) ProposeCmd(ctx context.Context, cmd []byte) (raft.Future, error) {
	future, err := r.cmdToFuture(cmd, commonpb.EntryNormal)

	if err != nil {
		return nil, err
	}

	select {
	case r.queue <- future:
		if r.metricsEnabled {
			rmetrics.writeReqs.Add(1)
		}
		return future, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ReadCmd implements raft.Raft.
func (r *Raft) ReadCmd(ctx context.Context, cmd []byte) (raft.Future, error) {
	future, err := r.cmdToFuture(cmd, commonpb.EntryNormal)

	if err != nil {
		return nil, err
	}

	if r.metricsEnabled {
		rmetrics.readReqs.Add(1)
	}

	r.Lock()
	r.pendingReads = append(r.pendingReads, future)
	r.Unlock()

	return future, nil
}
