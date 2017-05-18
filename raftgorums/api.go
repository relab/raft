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

	promise, future, err := r.cmdToFuture(cmd, commonpb.EntryReconf)

	// TODO Fix error returned here, NotLeader should be a status code.
	if err != nil {
		return nil, err
	}

	if !r.mem.startReconfiguration(req) {
		promise.Respond(&commonpb.ReconfResponse{
			Status: commonpb.ReconfTimeout,
		})
		return future, nil
	}

	r.incCmd()

	switch req.ReconfType {
	case commonpb.ReconfAdd:
		r.event.Record(raft.EventAddServer)
		go r.replicate(req.ServerID, promise)
	case commonpb.ReconfRemove:
		r.event.Record(raft.EventRemoveServer)
		r.queue <- promise
	}

	return future, nil
}

// ProposeCmd implements raft.Raft.
func (r *Raft) ProposeCmd(ctx context.Context, cmd []byte) (raft.Future, error) {
	promise, future, err := r.cmdToFuture(cmd, commonpb.EntryNormal)

	if err != nil {
		return nil, err
	}

	r.incCmd()

	select {
	case r.queue <- promise:
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
	promise, future, err := r.cmdToFuture(cmd, commonpb.EntryNormal)

	if err != nil {
		return nil, err
	}

	if r.metricsEnabled {
		rmetrics.readReqs.Add(1)
	}

	r.mu.Lock()
	r.pendingReads = append(r.pendingReads, promise.Read())
	r.mu.Unlock()

	return future, nil
}
