package raft

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/relab/raft/raftpb"
)

// State represents one of the Raft server states.
type State int

// Server states.
const (
	Follower State = iota
	Candidate
	Leader
)

//go:generate stringer -type=State

// Timeouts in milliseconds.
const (
	// How long we wait for an answer.
	TCPConnect   = 5000
	TCPHeartbeat = 2000
)

// None represents no server.
const None = 0

// BufferSize is the initial buffer size used for maps and buffered channels
// that directly depend on the number of requests being serviced.
const BufferSize = 10000

var (
	// ErrLateCommit indicates an entry taking too long to commit.
	ErrLateCommit = errors.New("Entry not committed in time.")
)

// Config contains the configuration need to start a Replica.
type Config struct {
	ID uint64

	Nodes []string

	Storage Storage

	Batch            bool
	QRPC             bool
	SlowQuorum       bool
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	MaxAppendEntries int

	Logger *log.Logger
}

// UniqueCommand identifies a client command.
type UniqueCommand struct {
	ClientID       uint32
	SequenceNumber uint64
}

// Persistent stores the persistent state of the Replica.
type Persistent struct {
	CurrentTerm uint64
	VotedFor    uint64
	Log         []*pb.Entry
	// TODO commands is recreated from entries on load. This should not be
	// needed. A client should, on leader crash, try the request with the
	// new leader.
	Commands map[UniqueCommand]*pb.ClientCommandRequest
}

// Replica represents a Raft server.
type Replica struct {
	// Must be acquired before mutating Replica state.
	sync.Mutex

	id     uint64
	leader uint64

	persistent *Persistent

	storage Storage

	seenLeader      bool
	heardFromLeader bool

	state State

	addrs []string

	commitIndex uint64

	nextIndex  int
	matchIndex int

	baselineTimeout  time.Duration
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	baseline  Timer
	election  Timer
	heartbeat Timer

	preElection bool

	pending map[UniqueCommand]chan<- *pb.ClientCommandRequest

	maxAppendEntries int
	queue            chan *pb.Entry

	batch bool

	rvreqout chan *pb.RequestVoteRequest
	aereqout chan *pb.AppendEntriesRequest

	logger *Logger
}

// NewReplica returns a new Replica given a configuration.
func NewReplica(cfg *Config) (*Replica, error) {
	// TODO Validate config, i.e., make sure to sensible defaults if an
	// option is not configured.
	if cfg.Logger == nil {
		cfg.Logger = log.New(ioutil.Discard, "", 0)
	}

	electionTimeout := randomTimeout(cfg.ElectionTimeout)

	heartbeat := NewTimer(0)
	heartbeat.Stop()

	var nodes []string

	// Remove self
	for _, node := range cfg.Nodes {
		if node == cfg.Nodes[cfg.ID-1] {
			continue
		}

		nodes = append(nodes, node)
	}

	persistent := cfg.Storage.Load()

	// TODO Order.
	r := &Replica{
		id:               cfg.ID,
		persistent:       &persistent,
		storage:          cfg.Storage,
		batch:            cfg.Batch,
		addrs:            nodes,
		nextIndex:        1,
		baselineTimeout:  cfg.ElectionTimeout,
		electionTimeout:  electionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		// TODO Fix Timer. Create new Timer instead of reusing.
		baseline:         NewTimer(cfg.ElectionTimeout),
		election:         NewTimer(randomTimeout(cfg.ElectionTimeout)),
		heartbeat:        heartbeat,
		preElection:      true,
		pending:          make(map[UniqueCommand]chan<- *pb.ClientCommandRequest, BufferSize),
		maxAppendEntries: cfg.MaxAppendEntries,
		queue:            make(chan *pb.Entry, BufferSize),
		rvreqout:         make(chan *pb.RequestVoteRequest, BufferSize),
		aereqout:         make(chan *pb.AppendEntriesRequest, BufferSize),
		logger:           &Logger{cfg.ID, cfg.Logger},
	}

	r.logger.Printf("ElectionTimeout: %v", r.electionTimeout)

	// Makes sure no RPCs are processed until the replica is ready.
	r.Lock()

	return r, nil
}

// RequestVoteRequestChan returns a channel for outgoing RequestVoteRequests.
// It's the implementers responsibility to make sure these requests are
// delivered.
func (r *Replica) RequestVoteRequestChan() chan *pb.RequestVoteRequest {
	return r.rvreqout
}

// AppendEntriesRequestChan returns a channel for outgoing RequestVoteRequests.
// It's the implementers responsibility to make sure these requests are
// delivered.
func (r *Replica) AppendEntriesRequestChan() chan *pb.AppendEntriesRequest {
	return r.aereqout
}

// Run handles timeouts.
// All RPCs are handled by Gorums.
func (r *Replica) Run() error {
	// Replica is ready.
	r.Unlock()

	for {
		select {
		case <-r.baseline.C:
			r.heardFromLeader = false
		case <-r.election.C:
			// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader
			// or granting vote to candidate: convert to candidate.
			r.startElection()

		case <-r.heartbeat.C:
			r.sendAppendEntries()
		}
	}
}

// HandleRequestVoteRequest must be called when receiving a RequestVoteRequest,
// the return value must be delivered to the requester.
func (r *Replica) HandleRequestVoteRequest(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	r.Lock()
	defer r.Unlock()

	pre := ""

	if req.PreVote {
		pre = "Pre"
	}

	if logLevel >= INFO {
		r.logger.from(req.CandidateID, fmt.Sprintf("%sVote requested in term %d for term %d", pre, r.persistent.CurrentTerm, req.Term))
	}

	// #RV1 Reply false if term < currentTerm.
	if req.Term < r.persistent.CurrentTerm {
		return &pb.RequestVoteResponse{Term: r.persistent.CurrentTerm}
	}

	if req.PreVote && r.heardFromLeader {
		// We don't grant pre-votes if we have recently heard from a
		// leader.
		return &pb.RequestVoteResponse{Term: r.persistent.CurrentTerm}
	}

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if req.Term > r.persistent.CurrentTerm && !req.PreVote {
		r.becomeFollower(req.Term)
	}

	notVotedYet := r.persistent.VotedFor == None

	// We can grant a vote in the same term, as long as it's to the same
	// candidate. This is useful if the response was lost, and the candidate
	// sends another request.
	alreadyVotedForCandidate := r.persistent.VotedFor == req.CandidateID

	// If the logs have last entries with different terms, the log with the
	// later term is more up-to-date.
	laterTerm := req.LastLogTerm > r.logTerm(len(r.persistent.Log))

	// If the logs end with the same term, whichever log is longer is more
	// up-to-date.
	longEnough := req.LastLogTerm == r.logTerm(len(r.persistent.Log)) && req.LastLogIndex >= uint64(len(r.persistent.Log))

	// We can only grant a vote if: we have not voted yet, we vote for the
	// same candidate again, or this is a pre-vote.
	canGrantVote := notVotedYet || alreadyVotedForCandidate || req.PreVote

	// #RV2 If votedFor is null or candidateId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote.
	if canGrantVote && (laterTerm || longEnough) {
		if logLevel >= INFO {
			r.logger.to(req.CandidateID, fmt.Sprintf("%sVote granted for term %d", pre, req.Term))
		}

		if req.PreVote {
			return &pb.RequestVoteResponse{VoteGranted: true, Term: req.Term}
		}

		r.persistent.VotedFor = req.CandidateID
		err := r.storage.SaveState(r.persistent.CurrentTerm, req.CandidateID)

		if err != nil {
			panic(fmt.Errorf("couldn't save state: %v", err))
		}

		// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader or granting a vote to candidate: convert to candidate.
		// Here we are granting a vote to a candidate so we reset the election timeout.
		r.election.Reset(r.electionTimeout)
		r.baseline.Reset(r.baselineTimeout)

		return &pb.RequestVoteResponse{VoteGranted: true, Term: r.persistent.CurrentTerm}
	}

	// #RV2 The candidate's log was not up-to-date
	return &pb.RequestVoteResponse{Term: r.persistent.CurrentTerm}
}

// HandleAppendEntriesRequest must be called when receiving a
// AppendEntriesRequest, the return value must be delivered to the requester.
func (r *Replica) HandleAppendEntriesRequest(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	r.Lock()
	defer r.Unlock()

	if logLevel >= TRACE {
		r.logger.from(req.LeaderID, fmt.Sprintf("AppendEntries = %+v", req))
	}

	// #AE1 Reply false if term < currentTerm.
	if req.Term < r.persistent.CurrentTerm {
		return &pb.AppendEntriesResponse{
			Success: false,
			Term:    req.Term,
		}
	}

	success := req.PrevLogIndex == 0 || (req.PrevLogIndex-1 < uint64(len(r.persistent.Log)) && r.persistent.Log[req.PrevLogIndex-1].Term == req.PrevLogTerm)

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if req.Term > r.persistent.CurrentTerm {
		r.becomeFollower(req.Term)
	} else if r.id != req.LeaderID {
		r.becomeFollower(r.persistent.CurrentTerm)
	}

	if success {
		r.leader = req.LeaderID
		r.heardFromLeader = true
		r.seenLeader = true

		index := int(req.PrevLogIndex)

		var toSave []*pb.Entry

		for _, entry := range req.Entries {
			index++

			if index == len(r.persistent.Log) || r.logTerm(index) != entry.Term {
				r.persistent.Log = r.persistent.Log[:index-1] // Remove excessive log entries.
				r.persistent.Log = append(r.persistent.Log, entry)

				toSave = append(toSave, entry)
			}
		}

		err := r.storage.SaveEntries(toSave)

		if err != nil {
			panic(fmt.Errorf("couldn't save entries: %v", err))
		}

		if logLevel >= DEBUG {
			r.logger.to(req.LeaderID, fmt.Sprintf("AppendEntries persisted %d entries to stable storage", len(req.Entries)))
		}

		old := r.commitIndex

		r.commitIndex = min(req.CommitIndex, uint64(index))

		if r.commitIndex > old {
			r.newCommit(old)
		}
	}

	return &pb.AppendEntriesResponse{
		Term:       req.Term,
		MatchIndex: uint64(len(r.persistent.Log)),
		Success:    success,
	}
}

// HandleClientCommandRequest must be called when receiving a
// ClientCommandRequest, the return value must be delivered to the requester.
func (r *Replica) HandleClientCommandRequest(request *pb.ClientCommandRequest) (*pb.ClientCommandResponse, error) {
	if response, isLeader := r.logCommand(request); isLeader {
		if !r.batch {
			r.sendAppendEntries()
		}

		select {
		// Wait on committed entry.
		case entry := <-response:
			return &pb.ClientCommandResponse{Status: pb.OK, Response: entry.Command, ClientID: entry.ClientID}, nil

		// Return if responding takes too much time.
		// The client will retry.
		case <-time.After(TCPHeartbeat * time.Millisecond):
			return nil, ErrLateCommit
		}
	}

	hint := r.getHint()

	return &pb.ClientCommandResponse{Status: pb.NOT_LEADER, LeaderHint: hint}, nil
}

func (r *Replica) logCommand(request *pb.ClientCommandRequest) (<-chan *pb.ClientCommandRequest, bool) {
	r.Lock()
	defer r.Unlock()

	if r.state == Leader {
		if request.SequenceNumber == 0 {
			request.ClientID = rand.Uint32()

			if logLevel >= INFO {
				r.logger.from(uint64(request.ClientID), fmt.Sprintf("Client request = %s", request.Command))
			}
		} else if logLevel >= TRACE {
			r.logger.from(uint64(request.ClientID), fmt.Sprintf("Client request = %s", request.Command))
		}

		commandID := UniqueCommand{ClientID: request.ClientID, SequenceNumber: request.SequenceNumber}

		if old, ok := r.persistent.Commands[commandID]; ok {
			response := make(chan *pb.ClientCommandRequest, 1)
			response <- old

			return response, true
		}

		r.Unlock()
		r.queue <- &pb.Entry{Term: r.persistent.CurrentTerm, Data: request}
		r.Lock()

		response := make(chan *pb.ClientCommandRequest)
		r.pending[commandID] = response

		return response, true
	}

	return nil, false
}

func (r *Replica) advanceCommitIndex() {
	r.Lock()
	defer r.Unlock()

	old := r.commitIndex

	if r.state == Leader && r.logTerm(r.matchIndex) == r.persistent.CurrentTerm {
		r.commitIndex = max(r.commitIndex, uint64(r.matchIndex))
	}

	if r.commitIndex > old {
		r.newCommit(old)
	}
}

// TODO Assumes caller already holds lock on Replica
func (r *Replica) newCommit(old uint64) {
	for i := old; i < r.commitIndex; i++ {
		committed := r.persistent.Log[i]
		sequenceNumber := committed.Data.SequenceNumber
		clientID := committed.Data.ClientID
		commandID := UniqueCommand{ClientID: clientID, SequenceNumber: sequenceNumber}

		if response, ok := r.pending[commandID]; ok {
			// If entry is not committed fast enough, the client
			// will retry.
			go func(response chan<- *pb.ClientCommandRequest) {
				select {
				case response <- committed.Data:
				case <-time.After(TCPHeartbeat * time.Millisecond):
				}
			}(response)
		}

		r.persistent.Commands[commandID] = committed.Data
	}
}

func (r *Replica) startElection() {
	r.Lock()
	defer r.Unlock()

	r.state = Candidate
	r.electionTimeout = randomTimeout(r.electionTimeout)

	term := r.persistent.CurrentTerm + 1
	pre := "pre"

	if !r.preElection {
		pre = ""
		// We are now a candidate. See Raft Paper Figure 2 -> Rules for Servers -> Candidates.
		// #C1 Increment currentTerm.
		r.persistent.CurrentTerm++

		err := r.storage.SaveState(r.persistent.CurrentTerm, r.id)

		if err != nil {
			panic(fmt.Errorf("couldn't save state: %v", err))
		}
	}

	if logLevel >= INFO {
		r.logger.log(fmt.Sprintf("Starting %s election for term %d", pre, term))
	}

	// #C3 Reset election timer.
	r.election.Reset(r.electionTimeout)

	// #C4 Send RequestVote RPCs to all other servers.
	r.rvreqout <- &pb.RequestVoteRequest{
		CandidateID:  r.id,
		Term:         term,
		LastLogTerm:  r.logTerm(len(r.persistent.Log)),
		LastLogIndex: uint64(len(r.persistent.Log)),
		PreVote:      r.preElection,
	}

	// Election is now started. Election will be continued in handleRequestVote when a response from Gorums is received.
	// See RequestVoteQF for the quorum function creating the response.
}

// HandleRequestVoteResponse must be invoked when receiving a
// RequestVoteResponse.
func (r *Replica) HandleRequestVoteResponse(response *pb.RequestVoteResponse) {
	r.Lock()
	defer r.Unlock()

	term := r.persistent.CurrentTerm

	if r.preElection {
		term++
	}

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if response.Term > term {
		r.becomeFollower(response.Term)

		return
	}

	// Ignore late response
	if response.Term < term {
		return
	}

	// Cont. from startElection(). We have now received a response from Gorums.

	// #C5 If votes received from majority of server: become leader.
	// Make sure we have not stepped down while waiting for replies.
	if r.state == Candidate && response.VoteGranted {
		if r.preElection {
			r.preElection = false
			// Start real election.
			r.election.Reset(0)

			return
		}

		// We have received at least a quorum of votes.
		// We are the leader for this term. See Raft Paper Figure 2 -> Rules for Servers -> Leaders.
		if logLevel >= INFO {
			r.logger.log(fmt.Sprintf("Elected leader for term %d", r.persistent.CurrentTerm))
		}

		r.state = Leader
		r.leader = r.id
		r.seenLeader = true
		r.nextIndex = len(r.persistent.Log) + 1

		// #L1 Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
		r.heartbeat.Reset(0)

		r.election.Stop()
		r.baseline.Stop()

		return
	}

	// #C7 If election timeout elapses: start new election.
	// This will happened if we don't receive enough replies in time. Or we lose the election but don't see a higher term number.
}

func (r *Replica) sendAppendEntries() {
	r.Lock()
	defer r.Unlock()

	if logLevel >= DEBUG {
		r.logger.log(fmt.Sprintf("Sending AppendEntries for term %d", r.persistent.CurrentTerm))
	}

	var toSave []*pb.Entry

LOOP:
	for i := r.maxAppendEntries; i > 0; i-- {
		select {
		case entry := <-r.queue:
			r.persistent.Log = append(r.persistent.Log, entry)
			toSave = append(toSave, entry)
		default:
			break LOOP
		}
	}

	err := r.storage.SaveEntries(toSave)

	if err != nil {
		panic(fmt.Errorf("couldn't save entries: %v", err))
	}

	// #L1
	entries := []*pb.Entry{}

	next := r.nextIndex - 1

	if len(r.persistent.Log) > next {
		maxEntries := int(min(uint64(next+r.maxAppendEntries), uint64(len(r.persistent.Log))))

		if !r.batch {
			maxEntries = next + 1
		}

		entries = r.persistent.Log[next:maxEntries]
	}

	if logLevel >= DEBUG {
		r.logger.log(fmt.Sprintf("Sending %d entries", len(entries)))
	}

	r.aereqout <- &pb.AppendEntriesRequest{
		LeaderID:     r.id,
		Term:         r.persistent.CurrentTerm,
		PrevLogIndex: uint64(next),
		PrevLogTerm:  r.logTerm(next),
		CommitIndex:  r.commitIndex,
		Entries:      entries,
	}

	r.heartbeat.Reset(r.heartbeatTimeout)
}

// HandleAppendEntriesResponse must be invoked when receiving an
// AppendEntriesResponse.
func (r *Replica) HandleAppendEntriesResponse(response *pb.AppendEntriesResponse) {
	r.Lock()
	defer func() {
		r.Unlock()
		r.advanceCommitIndex()
	}()

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if response.Term > r.persistent.CurrentTerm {
		r.becomeFollower(response.Term)

		return
	}

	// Ignore late response
	if response.Term < r.persistent.CurrentTerm {
		return
	}

	if r.state == Leader {
		if response.Success {
			r.matchIndex = int(response.MatchIndex)
			r.nextIndex = r.matchIndex + 1

			return
		}

		// If AppendEntries was not successful lower match index.
		r.nextIndex = int(max(1, response.MatchIndex))
	}
}

// TODO Tests.
func (r *Replica) becomeFollower(term uint64) {
	r.state = Follower
	r.preElection = true

	if r.persistent.CurrentTerm != term {
		if logLevel >= INFO {
			r.logger.log(fmt.Sprintf("Become follower as we transition from term %d to %d", r.persistent.CurrentTerm, term))
		}

		r.persistent.CurrentTerm = term
		r.persistent.VotedFor = None

		err := r.storage.SaveState(term, r.persistent.VotedFor)

		if err != nil {
			panic(fmt.Errorf("couldn't save state: %v", err))
		}
	}

	r.election.Reset(r.electionTimeout)
	r.baseline.Reset(r.baselineTimeout)
	r.heartbeat.Stop()
}

func (r *Replica) getHint() uint32 {
	r.Lock()
	defer r.Unlock()

	var hint uint32

	if r.seenLeader {
		hint = uint32(r.leader)
	}

	// If client receives hint = 0, it should try another random server.
	return hint
}

func (r *Replica) logTerm(index int) uint64 {
	if index < 1 || index > len(r.persistent.Log) {
		return 0
	}

	return r.persistent.Log[index-1].Term
}
