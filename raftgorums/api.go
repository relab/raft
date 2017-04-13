package raftgorums

import (
	"golang.org/x/net/context"

	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
)

// ProposeConf implements raft.Raft.
func (r *Raft) ProposeConf(ctx context.Context, req *commonpb.ReconfRequest) (raft.Future, error) {
	cmd, err := req.Marshal()

	if err != nil {
		return nil, err
	}

	future, err := r.cmdToFuture(cmd, commonpb.EntryConfChange)

	// TODO Fix error returned here, NotLeader should be a status code.
	if err != nil {
		return nil, err
	}

	if !r.mem.startReconfiguration(req) {
		future.Respond(&commonpb.ReconfResponse{
			Status: commonpb.ReconfTimeout,
		})
		return future, nil
	}

	switch req.ReconfType {
	case commonpb.ReconfAdd:
		go r.replicate(req.ServerID, future)
	case commonpb.ReconfRemove:
		r.queue <- future
	}

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
