package raft

import pb "github.com/relab/raft/pkg/raft/raftpb"

// QuorumSpec holds information about the quorum size of the current
// configuration and allows us to invoke QRPCs.
type QuorumSpec struct {
	N    int
	Q    int
	MajQ int
}

// RequestVoteQF gathers RequestVoteResponses and delivers a reply when a higher
// term is seen or a quorum of votes is received.
func (qs *QuorumSpec) RequestVoteQF(req *pb.RequestVoteRequest, replies []*pb.RequestVoteResponse) (*pb.RequestVoteResponse, bool) {
	votes := 0
	response := *replies[len(replies)-1]

	if response.Term > req.Term {
		return &response, true
	}

	for _, reply := range replies {
		if reply.VoteGranted {
			votes++
		}

	}

	if votes >= qs.MajQ {
		response.VoteGranted = true

		return &response, true
	}

	return nil, false
}

// AppendEntriesQF gathers AppendEntriesResponses and calculates the log entries
// replicated, depending on the quorum configuration.
func (qs *QuorumSpec) AppendEntriesQF(req *pb.AppendEntriesRequest, replies []*pb.AppendEntriesResponse) (*pb.AppendEntriesResponse, bool) {
	last := replies[len(replies)-1]

	// Abort.
	if last.Term > req.Term {
		return last, true
	}

	reply, successful := qs.combine(replies)
	reply.Term = req.Term

	// The request was successful if we received a quorum, or a response
	// from all the servers.
	done := reply.Success || len(replies) == qs.N

	// done will be false until successful >= qs.Q or we receive a response
	// from every server. This makes sure a request that times out will be
	// able to proceed with only a majority quorum. Note that we don't allow
	// the cluster to proceed until every server that responds is
	// successful. This cause all servers to stay up-to-date.
	if len(replies) == successful {
		reply.Success = successful >= qs.MajQ
	}

	// If an AppendEntries is unsuccessful every nodes next index must be
	// reset to the lowest seen match index. Setting the FollowerID to nil
	// is not strictly need here but it allows us to know that the reply
	// came from a quorum call. A direct reply always has exactly one
	// FollowerID.
	if !reply.Success {
		reply.FollowerID = nil
	}

	return reply, done
}

func (qs *QuorumSpec) combine(replies []*pb.AppendEntriesResponse) (*pb.AppendEntriesResponse, int) {
	successful := 0
	minMatch := ^uint64(0) // Largest uint64.
	reply := &pb.AppendEntriesResponse{}

	for _, r := range replies {
		if r.MatchIndex < minMatch {
			minMatch = r.MatchIndex
		}

		if r.Success {
			reply.MatchIndex = r.MatchIndex
			reply.FollowerID = append(reply.FollowerID, r.FollowerID...)
			successful++
		}
	}

	// If there were enough successful responses, we have a quorum.
	reply.Success = successful >= qs.Q

	// If we don't have a quorum, set match index to the lowest seen.
	if !reply.Success {
		reply.MatchIndex = minMatch
	}

	return reply, successful
}
