package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/relab/gorums/idutil"
	"github.com/relab/raft/debug"
	"github.com/relab/raft/proto/gorums"
)

// Represents one of the Raft server states.
type State int

// Server states.
// TODO: Generator?
const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

// Timeouts in milliseconds.
const (
	HEARTBEAT = 50
	ELECTION  = 150
)

const NONE = -1

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func randomTimeout() time.Duration {
	return time.Duration(ELECTION+rand.Intn(ELECTION*2-ELECTION)) * time.Millisecond
}

type Replica struct {
	sync.Mutex

	id     int64
	leader int64

	state State

	conf  *gorums.Configuration
	confs []*gorums.Configuration

	votedFor    int64
	currentTerm uint64

	log []*gorums.Entry

	commitIndex int

	nextIndex  []int
	matchIndex []int

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	election  Timer
	heartbeat Timer
}

func (r *Replica) logTerm(index int) uint64 {
	if index < 1 || index > len(r.log) {
		return 0
	}

	return r.log[index].Term
}

func (r *Replica) Init(nodes []string) error {
	mgr, err := gorums.NewManager(nodes,
		gorums.WithGrpcDialOptions(
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(time.Second*10)))

	if err != nil {
		return err
	}

	qspec := &QuorumSpec{
		N: len(mgr.NodeIDs()),
		Q: len(mgr.NodeIDs())/2 + 1,
	}

	conf, err := mgr.NewConfiguration(mgr.NodeIDs(), qspec, time.Second)

	if err != nil {
		return err
	}

	r.conf = conf

	id, err := idutil.IDFromAddress(nodes[0])

	if err != nil {
		return err
	}

	for i, nid := range mgr.NodeIDs() {
		if id == nid {
			r.id = int64(i)
		}

		conf, err := mgr.NewConfiguration([]uint32{nid}, qspec, time.Second)

		if err != nil {
			return err
		}

		r.confs = append(r.confs, conf)
	}

	r.electionTimeout = randomTimeout()
	r.heartbeatTimeout = HEARTBEAT * time.Millisecond

	debug.Debugln(r.id, ":: TIMEOUT SET,", r.electionTimeout)

	r.election = NewTimer(r.electionTimeout)
	r.heartbeat = NewTimer(0)
	r.heartbeat.Stop()

	r.votedFor = NONE

	peers := len(mgr.NodeIDs())

	r.nextIndex = make([]int, peers)
	r.matchIndex = make([]int, peers)

	// Initialized to leader last log index + 1.
	for i := range r.matchIndex {
		r.matchIndex[i] = 1
	}

	r.Unlock()

	return nil
}

func (r *Replica) Run() {
	for {
		select {
		case <-r.election.C:
			// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader
			// or granting vote to candidate: convert to candidate.
			r.startElection()

		case <-r.heartbeat.C:
			r.sendAppendEntries()
		}
	}
}

func (r *Replica) RequestVote(ctx context.Context, request *gorums.RequestVoteRequest) (*gorums.RequestVoteResponse, error) {
	r.Lock()
	defer r.Unlock()

	debug.Debugln(r.id, ":: VOTE REQUESTED, from", request.CandidateID, "for term", request.Term)

	// #RV1 Reply false if term < currentTerm.
	if request.Term < r.currentTerm {
		return &gorums.RequestVoteResponse{VoteGranted: false, Term: r.currentTerm}, nil
	}

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if request.Term > r.currentTerm {
		r.becomeFollower(request.Term)

		debug.Debugln(r.id, ":: VOTE GRANTED, to", request.CandidateID, "for term", request.Term)

		r.votedFor = request.CandidateID

		return &gorums.RequestVoteResponse{VoteGranted: true, Term: r.currentTerm}, nil
	}

	// #RV2 If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
	// TODO: Is len(r.log) off by one?
	if request.Term == r.currentTerm && r.votedFor == NONE || r.votedFor == request.CandidateID &&
		(request.LastLogTerm > r.logTerm(len(r.log)) ||
			(request.LastLogTerm == r.logTerm(len(r.log)) && request.LastLogIndex >= uint64(len(r.log)))) {
		debug.Debugln(r.id, ":: VOTE GRANTED, to", request.CandidateID, "for term", request.Term)

		r.votedFor = request.CandidateID

		// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader or granting a vote to candidate: convert to candidate.
		// Here we are granting a vote to a candidate so we reset the election timeout.
		r.election.Reset(r.electionTimeout)

		return &gorums.RequestVoteResponse{VoteGranted: true, Term: r.currentTerm}, nil
	}

	// #RV2 The candidate's log was not up-to-date
	return &gorums.RequestVoteResponse{VoteGranted: false, Term: r.currentTerm}, nil
}

func (r *Replica) AppendEntries(ctx context.Context, request *gorums.AppendEntriesRequest) (*gorums.AppendEntriesResponse, error) {
	r.Lock()
	defer r.Unlock()

	// #AE1 Reply false if term < currentTerm.
	if request.Term < r.currentTerm {
		return &gorums.AppendEntriesResponse{Success: false, Term: r.currentTerm}, nil
	}

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if request.Term > r.currentTerm {
		r.becomeFollower(request.Term)
	}

	r.leader = request.LeaderID
	r.state = FOLLOWER

	// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader or granting a vote to candidate: convert to candidate.
	// Here we are receiving AppendEntries RPC from the current leader so we reset the election timeout.
	r.election.Reset(r.electionTimeout)

	success := request.PrevLogIndex == 0 || (request.PrevLogIndex <= uint64(len(r.log)) && r.log[request.PrevLogIndex].Term == request.PrevLogTerm)

	if success {
		debug.Debugln(r.id, ":: OK", r.currentTerm)

		index := int(request.PrevLogIndex)

		for _, entry := range request.Entries {
			index++

			if r.logTerm(index) != entry.Term {
				r.log = r.log[:index-1] // Remove excessive log entries. TODO: Confirm index. Most likely we will have a off by one error.
				r.log = append(r.log, entry)
			}

			r.commitIndex = min(int(request.CommitIndex), index)
		}
	}

	return &gorums.AppendEntriesResponse{Success: success, Term: r.currentTerm}, nil
}

func (r *Replica) startElection() {
	r.Lock()
	defer r.Unlock()

	r.state = CANDIDATE
	r.electionTimeout = randomTimeout()

	// We are now a candidate. See Raft Paper Figure 2 -> Rules for Servers -> Candidates.
	// #C1 Increment currentTerm.
	r.currentTerm++

	debug.Debugln(r.id, ":: ELECTION STARTED, for term", r.currentTerm)

	// #C3 Reset election timer.
	r.election.Reset(r.electionTimeout)

	// #C4 Send RequestVote RPCs to all other servers.
	req := r.conf.RequestVoteFuture(&gorums.RequestVoteRequest{CandidateID: r.id, Term: r.currentTerm})

	go func() {
		reply, err := req.Get()

		if err != nil {
			log.Println(err)
		} else {
			r.handleRequestVoteResponse(reply.Reply)
		}
	}()

	// Election is now started. Election will be continued in handleRequestVote when a response from Gorums is received.
	// See RequestVoteQF for the quorum function creating the response.
}

func (r *Replica) handleRequestVoteResponse(response *gorums.RequestVoteResponse) {
	r.Lock()
	defer r.Unlock()

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if response.Term > r.currentTerm {
		r.becomeFollower(response.Term)

		return
	}

	// Ignore late response
	if response.Term < r.currentTerm {
		return
	}

	// Cont. from startElection(). We have now received a response from Gorums.

	// #C5 If votes received from majority of server: become leader.
	// Make sure we have not stepped down while waiting for replies.
	if response.VoteGranted && r.state == CANDIDATE {
		// We have received at least a quorum of votes.
		// We are the leader for this term. See Raft Paper Figure 2 -> Rules for Servers -> Leaders.

		debug.Debugln(r.id, ":: ELECTED LEADER, for term", r.currentTerm)

		r.state = LEADER
		r.leader = r.id

		for i := range r.nextIndex {
			r.nextIndex[i] = len(r.log)
		}

		// #L1 Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
		r.heartbeat.Reset(0)

		r.election.Stop()

		return
	}

	// TODO: We didn't win the election. We should continue sending AppendEntries RPCs until the election runs out.

	// #C7 If election timeout elapses: start new election.
	// This will happened if we don't receive enough replies in time. Or we lose the election but don't see a higher term number.
}

func (r *Replica) sendAppendEntries() {
	r.Lock()
	defer r.Unlock()

	debug.Debugln(r.id, ":: APPENDENTRIES, for term", r.currentTerm)

	// #L1
	for _, conf := range r.confs {
		req := conf.AppendEntriesFuture(&gorums.AppendEntriesRequest{LeaderID: r.id, Term: r.currentTerm})

		go func(req *gorums.AppendEntriesFuture) {
			reply, err := req.Get()

			if err != nil {
				log.Println(err)
			} else {
				r.handleAppendEntriesResponse(reply.Reply)
			}
		}(req)
	}

	r.heartbeat.Reset(r.heartbeatTimeout)
}

func (r *Replica) handleAppendEntriesResponse(response *gorums.AppendEntriesResponse) {
	r.Lock()
	defer r.Unlock()

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if response.Term > r.currentTerm {
		r.becomeFollower(response.Term)

		return
	}

	// TODO: Deal with AppendEntries response.
}

func (r *Replica) becomeFollower(term uint64) {
	debug.Debugln(r.id, ":: STEPDOWN,", r.currentTerm, "->", term)

	r.state = FOLLOWER
	r.currentTerm = term
	r.votedFor = NONE

	r.election.Reset(r.electionTimeout)
	r.heartbeat.Stop()
}
