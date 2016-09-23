package main

import (
	"log"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/relab/raft/proto/gorums"
	"google.golang.org/grpc"
)

type node struct {
	sync.Mutex

	id uint32

	votedFor uint32

	currentTerm uint64

	conf *gorums.Configuration

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	election  *time.Timer
	heartbeat *time.Timer

	done chan struct{}
}

func (n *node) ReqVote(ctx context.Context, request *gorums.RequestVote_Request) (*gorums.RequestVote_Response, error) {
	n.Lock()
	defer n.Unlock()

	// DEBUG
	log.Println(request.CandidateID, "requesting vote from", n.id, "for term", request.Term)

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if request.Term > n.currentTerm {
		n.currentTerm = request.Term
		n.votedFor = 0

		// Stop heartbeat timeout. TODO: Confirm safety.
		if !n.heartbeat.Stop() {
			select {
			case <-n.heartbeat.C:
			default:
			}
		}
	}

	// #RV1 Reply false if term < currentTerm.
	if request.Term < n.currentTerm {
		return &gorums.RequestVote_Response{VoteGranted: false, Term: n.currentTerm}, nil
	}

	// #RV2 If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote. TODO: log part.
	// Make sure we don't double vote.
	// We can vote for the same candidate again (e.g. response was lost).
	// TODO: Make sure that no node can have id(address) = 0. Should probably change to -1.
	if n.votedFor != 0 && n.votedFor != request.CandidateID {
		return &gorums.RequestVote_Response{VoteGranted: false, Term: n.currentTerm}, nil
	}

	// DEBUG
	log.Println(n.id, "granted vote to", request.CandidateID, "in term", request.Term)

	n.votedFor = request.CandidateID

	// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader or granting a vote to candidate: convert to candidate.
	// Here we are granting a vote to a candidate so we reset the election timeout.
	if !n.election.Stop() {
		select {
		case <-n.election.C:
		default:
		}
	}

	n.election.Reset(n.electionTimeout)

	return &gorums.RequestVote_Response{VoteGranted: true, Term: n.currentTerm}, nil
}

func (n *node) AppEntries(ctx context.Context, request *gorums.AppendEntries_Request) (*gorums.AppendEntries_Response, error) {
	n.Lock()
	defer n.Unlock()

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if request.Term > n.currentTerm {
		n.currentTerm = request.Term
		n.votedFor = 0

		// Stop heartbeat timeout. TODO: Confirm safety.
		if !n.heartbeat.Stop() {
			select {
			case <-n.heartbeat.C:
			default:
			}
		}
	}

	// #AE1 Reply false if term < currentTerm.
	if request.Term < n.currentTerm {
		return &gorums.AppendEntries_Response{Success: false, Term: n.currentTerm}, nil
	}

	// DEBUG
	log.Println(n.id, "received AppendEntries from leader in term", n.currentTerm)

	// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader or granting a vote to candidate: convert to candidate.
	// Here we are receiving AppendEntries RPC from the current leader so we reset the election timeout.
	if !n.election.Stop() {
		select {
		case <-n.election.C:
		default:
		}
	}

	n.election.Reset(n.electionTimeout)

	return &gorums.AppendEntries_Response{Success: true, Term: n.currentTerm}, nil
}

func (n *node) handleRequestVote(response *gorums.RequestVote_Response) {
	n.Lock()
	defer n.Unlock()

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if response.Term > n.currentTerm {
		n.currentTerm = response.Term
		n.votedFor = 0

		// Stop heartbeat timeout. TODO: Confirm safety.
		if !n.heartbeat.Stop() {
			select {
			case <-n.heartbeat.C:
			default:
			}
		}
	}

	// Ignore late response
	if response.Term < n.currentTerm {
		return
	}

	// Cont. from startElection(). We have now received a response from Gorums.

	// #C5 If votes received from majority of server: become leader.
	if response.VoteGranted {
		// We have received at least a quorum of votes.
		// We are the leader for this term. See Raft Paper Figure 2 -> Rules for Servers -> Leaders.

		// DEBUG
		log.Println(n.id, "is elected leader for term", n.currentTerm)

		// #L1 Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
		// repeat during idle periods to prevent election timeouts. TODO: implement.

		// Reset heartbeat (forcing a immediate AppendEntries RPC).
		// It is safe as heartbeat must be stopped when we become a follower again. TODO: Confirm safety.
		n.heartbeat.Reset(0)

		// Stop election timeout. TODO: Start again when becoming a follower. TODO: Confirm safety.
		if !n.election.Stop() {
			select {
			case <-n.election.C:
			default:
			}
		}

		// TODO: This should be enough for now. We are only implementing the leader election.
		return
	}

	// #C6 If AppendEntries RPC received from new leader: convert to follower.
	// We didn't win the election but we have identified another leader for the term.
	// Step down to follower.
	// TODO: This needs to be dealt with in respondAppendEntries.
	// TODO: How do we deal with late responses?

	// #C7 If election timeout elapses: start new election.
	// This would have happened if we didn't receive a response in time.
}

func (n *node) handleAppendEntries(response *gorums.AppendEntries_Response) {
	n.Lock()
	defer n.Unlock()

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if response.Term > n.currentTerm {
		n.currentTerm = response.Term
		n.votedFor = 0

		// Stop heartbeat timeout. TODO: Confirm safety.
		if !n.heartbeat.Stop() {
			select {
			case <-n.heartbeat.C:
			default:
			}
		}
	}

	// TODO: Deal with AppendEntries response.
}

func (n *node) startElection() {
	n.Lock()
	defer n.Unlock()

	// We are now a candidate. See Raft Paper Figure 2 -> Rules for Servers -> Candidates.
	// #C1 Increment currentTerm.
	n.currentTerm++

	// DEBUG
	log.Println(n.id, "is starting a election for term", n.currentTerm)

	// #C2 Vote for self.
	// This is implicit. qs.q = Quorum - 1. TODO: Confirm that this actually works.
	// But we need to set n.votedFor.
	n.votedFor = n.id

	// #C3 Reset election timer.
	n.election.Reset(n.electionTimeout)

	// #C4 Send RequestVote RPCs to all other servers.
	req := n.conf.ReqVoteFuture(&gorums.RequestVote_Request{CandidateID: n.id, Term: n.currentTerm})

	go func() {
		reply, err := req.Get()

		if err != nil {
			// TODO: Can we safely ignore this as the protocol should just retry the election?
			log.Println(err)
		} else {
			n.handleRequestVote(reply.Reply)
		}
	}()

	// Election is now started. Election will be continued in handleRequestVote when a response from Gorums is received.
	// See ReqVoteQF for the quorum function creating the response.
}

func (n *node) sendAppendEntries() {
	n.Lock()
	defer n.Unlock()

	// DEBUG
	log.Println(n.id, "is sending AppendEntries to followers for term", n.currentTerm)

	// #L1
	req := n.conf.AppEntriesFuture(&gorums.AppendEntries_Request{LeaderID: n.id, Term: n.currentTerm})

	go func() {
		reply, err := req.Get()

		if err != nil {
			// TODO: Can we safely ignore this as the protocol should just retry the election?
			log.Println(err)
		} else {
			n.handleAppendEntries(reply.Reply)
		}
	}()

	// TODO: Later sendAppendEntries might not necessarily be called by a heartbeat timeout.
	// Make sure timer is in the correct state (stopped and drained).
	n.heartbeat.Reset(n.heartbeatTimeout)
}

func (n *node) Run(nodes []string) {
	mgr, err := gorums.NewManager(nodes,
		gorums.WithGrpcDialOptions(
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(time.Second*10)))

	if err != nil {
		log.Fatal(err)
	}

	qs := &raftQSpec{
		n: len(mgr.NodeIDs()) + 1,
		// Implicit vote from local node
		q: (len(mgr.NodeIDs()) + 1) / 2,
	}

	conf, err := mgr.NewConfiguration(mgr.NodeIDs(), qs, time.Second)

	if err != nil {
		log.Fatal(err)
	}

	n.conf = conf


OUT:
	for {
		select {
		case <-n.election.C:
			// DEBUG
			dlog("lock: election")

			// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader
			// or granting vote to candidate: convert to candidate.
			n.startElection()

			// DEBUG
			dlog("unlock: election")
		case <-n.heartbeat.C:
			// DEBUG
			dlog("lock: heartbeat")

			n.sendAppendEntries()

			// DEBUG
			dlog("unlock: heartbeat")
		case <-n.done:
			break OUT
		}
	}
}
