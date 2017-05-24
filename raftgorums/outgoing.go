package raftgorums

import (
	"time"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/relab/raft"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

func (r *Raft) handleOutgoing() error {
	// January 1, 1970 UTC.
	var lastCuReq time.Time

	recorded := make(map[uint32]struct{})

	for {
		select {
		case <-r.stop:
			return nil
		case err := <-r.mem.get().SubError():
			// TODO If a node becomes unavailable and there is a
			// backup available in the same or an alternate region,
			// instantiate reconfiguratior. TODO How many errors
			// before a node is considered unavailable? If there is
			// no backup node available, don't do anything, but
			// schedule the reconfiguratior.
			r.logger.WithField("nodeid", err.NodeID).Warnln("Node unavailable")

			if _, ok := recorded[err.NodeID]; !ok {
				recorded[err.NodeID] = struct{}{}
				r.event.Record(raft.EventFailure)
			}

		case req := <-r.cureqout:
			// TODO Use config.
			if time.Since(lastCuReq) < 100*time.Millisecond {
				continue
			}
			lastCuReq = time.Now()
			rmetrics.catchups.Add(1)

			r.logger.WithField("matchindex", req.matchIndex).Warnln("Sending catch-up")
			ctx, cancel := context.WithTimeout(context.Background(), r.electionTimeout)
			leader := r.mem.getNode(req.leaderID)
			r.event.Record(raft.EventCatchup)
			_, err := leader.RaftClient.CatchMeUp(ctx, &pb.CatchMeUpRequest{
				FollowerID: r.id,
				NextIndex:  req.matchIndex + 1,
			})
			cancel()

			if err != nil {
				r.logger.WithError(err).Warnln("CatchMeUp failed")
			}
		case req := <-r.rvreqout:
			conf := r.mem.get()

			r.logger.WithField("conf", conf.NodeIDs()).Println("Sending request for vote")

			ctx, cancel := context.WithTimeout(context.Background(), 10*r.electionTimeout)
			res, err := conf.RequestVote(ctx, req)
			cancel()

			if err != nil {
				r.logger.WithError(err).Warnln("RequestVote failed")
			}

			if res == nil {
				continue
			}

			r.HandleRequestVoteResponse(res)

		case req := <-r.aereqout:
			next := make(map[uint32]uint64)
			nextIndex := req.PrevLogIndex + 1

			for nodeID, ch := range r.match {
				if !r.mem.inLatest(nodeID) {
				EMPTY:
					// Empty channel.
					for {
						select {
						case <-ch:
						default:
							break EMPTY
						}
					}
					continue
				}
			LATEST:
				// Get latest catchup index.
				for {
					select {
					case index := <-ch:
						atLeastMaxEntries := req.PrevLogIndex+1 > r.burst
						tooFarBehind := index < req.PrevLogIndex+1-r.burst

						if atLeastMaxEntries && tooFarBehind {
							r.logger.WithFields(logrus.Fields{
								"gorumsid": nodeID,
							}).Warnln("Server too far behind")
							index = req.PrevLogIndex + 1
						}
						next[nodeID] = index
						if index < nextIndex {
							nextIndex = index
						}
					default:
						break LATEST
					}
				}
			}

			entries := r.getNextEntries(nextIndex)
			e := uint64(len(entries))
			maxIndex := nextIndex + e - 1

			conf := r.mem.get()
			r.logger.WithField("conf", conf.NodeIDs()).Println("Sending append entries request")

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			res, err := conf.AppendEntries(ctx, req,
				// These functions will be executed concurrently.
				func(req pb.AppendEntriesRequest, nodeID uint32) *pb.AppendEntriesRequest {
					if index, ok := next[nodeID]; ok {
						r.event.Record(raft.EventInjectEntries)
						req.PrevLogIndex = index - 1
						req.PrevLogTerm = r.logTerm(index - 1)
						req.Catchup = true
					}

					need := maxIndex - req.PrevLogIndex
					req.Entries = entries[e-need:]

					r.logger.WithFields(logrus.Fields{
						"prevlogindex": req.PrevLogIndex,
						"prevlogterm":  req.PrevLogTerm,
						"commitindex":  req.CommitIndex,
						"currentterm":  req.Term,
						"lenentries":   len(req.Entries),
						"gorumsid":     nodeID,
					}).Infoln("Sending AppendEntries")

					return &req
				},
			)

			if err != nil {
				r.logger.WithError(err).Warnln("AppendEntries failed")
			}

			if res == nil {
				// This will cause the leader to step down as
				// len(res.Replies) == 0.
				res = &pb.AppendEntriesQFResponse{
					Term: req.Term,
				}
			}

			// Cancel on abort.
			if !res.Success {
				cancel()
			}

			r.HandleAppendEntriesResponse(res, maxIndex)
		}
	}
}
