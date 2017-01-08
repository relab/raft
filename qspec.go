package raft

import pb "github.com/relab/raft/raftpb"

// QuorumSpec holds information about the quorum size of the current configuration
// and allows us to invoke QRPCs.
type QuorumSpec struct {
	N  int
	SQ int
	FQ int
}

// RequestVoteQF gathers RequestVoteResponses
// and delivers a reply when a higher term is seen or a quorum of votes is received.
func (qspec *QuorumSpec) RequestVoteQF(req *pb.RequestVoteRequest, replies []*pb.RequestVoteResponse) (*pb.RequestVoteResponse, bool) {
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

	if votes >= qspec.FQ {
		response.VoteGranted = true

		return &response, true
	}

	return nil, false
}

// AppendEntriesQF gathers AppendEntriesResponses
// and calculates the log entries replicated, depending on the quorum configuration.
func (qspec *QuorumSpec) AppendEntriesQF(req *pb.AppendEntriesRequest, replies []*pb.AppendEntriesResponse) (*pb.AppendEntriesResponse, bool) {
	numSuccess := 0
	maxMatchIndex := uint64(0)
	response := *replies[len(replies)-1]
	response.Success = false
	response.FollowerID = nil

	if response.Term > req.Term {
		return &response, true
	}

	for _, reply := range replies {
		if reply.MatchIndex < response.MatchIndex {
			response.MatchIndex = reply.MatchIndex
		}

		if reply.Success {
			maxMatchIndex = reply.MatchIndex
			numSuccess++
			response.FollowerID = append(response.FollowerID, reply.FollowerID[0])
		}
	}

	if numSuccess >= qspec.SQ {
		response.MatchIndex = maxMatchIndex
		response.Success = true

		return &response, true
	}

	// Majority quorum.
	quorum := numSuccess >= qspec.FQ

	// If we have a majority and all the replicas that responded were successful.
	if quorum && len(replies) == numSuccess {
		response.Success = true
	} else {
		// This is not needed but makes it clear that FollowerID should
		// not be used when AppendEntries fail.
		response.FollowerID = nil
	}

	// Wait for more replies but leave the response in the case of a timeout.
	// We do this so that the cluster can proceed even if some replicas are down.
	return &response, false
}
