package raft

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/relab/libraftgorums/raftpb"
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
	MaxAppendEntries uint64

	Logger *log.Logger
}

// UniqueCommand identifies a client command.
type UniqueCommand struct {
	ClientID       uint32
	SequenceNumber uint64
}

// Replica represents a Raft server.
type Replica struct {
	// Must be acquired before mutating Replica state.
	sync.Mutex

	id     uint64
	leader uint64

	currentTerm uint64
	votedFor    uint64

	storage Storage

	seenLeader      bool
	heardFromLeader bool

	state State

	addrs []string

	commitIndex uint64

	nextIndex  uint64
	matchIndex uint64

	baselineTimeout  time.Duration
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	baseline  *Timer
	election  *Timer
	heartbeat *Timer

	cyclech chan struct{}

	preElection bool

	pending map[UniqueCommand]chan<- *pb.ClientCommandRequest

	maxAppendEntries uint64
	queue            chan *pb.Entry
	commands         map[UniqueCommand]*pb.ClientCommandRequest

	batch bool

	rvreqout chan *pb.RequestVoteRequest
	aereqout chan *pb.AppendEntriesRequest

	logger *Logger
}

// NewReplica returns a new Replica given a configuration.
func NewReplica(cfg *Config) *Replica {
	// TODO Validate config, i.e., make sure to sensible defaults if an
	// option is not configured.
	if cfg.Logger == nil {
		cfg.Logger = log.New(ioutil.Discard, "", 0)
	}

	var nodes []string

	// Remove self
	for _, node := range cfg.Nodes {
		if node == cfg.Nodes[cfg.ID-1] {
			continue
		}

		nodes = append(nodes, node)
	}

	term, err := cfg.Storage.Get(KeyTerm)

	if err != nil {
		panic(fmt.Errorf("couldn't get Term: %v", err))
	}

	votedFor, err := cfg.Storage.Get(KeyVotedFor)

	if err != nil {
		panic(fmt.Errorf("couldn't get VotedFor: %v", err))
	}

	electionTimeout := randomTimeout(cfg.ElectionTimeout)
	heartbeat := NewTimer(cfg.HeartbeatTimeout)
	heartbeat.Disable()

	// TODO Order.
	r := &Replica{
		id:               cfg.ID,
		currentTerm:      term,
		votedFor:         votedFor,
		storage:          cfg.Storage,
		batch:            cfg.Batch,
		addrs:            nodes,
		nextIndex:        1,
		baselineTimeout:  cfg.ElectionTimeout,
		electionTimeout:  electionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		baseline:         NewTimer(cfg.ElectionTimeout),
		election:         NewTimer(electionTimeout),
		heartbeat:        heartbeat,
		cyclech:          make(chan struct{}, 128),
		preElection:      true,
		pending:          make(map[UniqueCommand]chan<- *pb.ClientCommandRequest, BufferSize),
		maxAppendEntries: cfg.MaxAppendEntries,
		queue:            make(chan *pb.Entry, BufferSize),
		commands:         make(map[UniqueCommand]*pb.ClientCommandRequest),
		rvreqout:         make(chan *pb.RequestVoteRequest, BufferSize),
		aereqout:         make(chan *pb.AppendEntriesRequest, BufferSize),
		logger:           &Logger{cfg.ID, cfg.Logger},
	}

	r.logger.Printf("ElectionTimeout: %v", r.electionTimeout)

	return r
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
func (r *Replica) Run() {
	for {
		select {
		case <-r.baseline.C:
			r.heardFromLeader = false
		case <-r.election.C:
			// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader
			// or granting vote to candidate: convert to candidate.
			r.startElection()

			// #C3 Reset election timer.
			r.election.Restart()
		case <-r.heartbeat.C:
			r.sendAppendEntries()
			r.heartbeat.Restart()
		case <-r.cyclech:
			// Refresh channel pointers.
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
		r.logger.from(req.CandidateID, fmt.Sprintf("%sVote requested in term %d for term %d", pre, r.currentTerm, req.Term))
	}

	// #RV1 Reply false if term < currentTerm.
	if req.Term < r.currentTerm {
		return &pb.RequestVoteResponse{Term: r.currentTerm}
	}

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if req.Term > r.currentTerm && !req.PreVote {
		r.becomeFollower(req.Term)
	}

	voted := r.votedFor != None

	if req.PreVote && (r.heardFromLeader || (voted && req.Term == r.currentTerm)) {
		// We don't grant pre-votes if we have recently heard from a
		// leader or already voted in the pre-term.
		return &pb.RequestVoteResponse{Term: r.currentTerm}
	}

	// We can grant a vote in the same term, as long as it's to the same
	// candidate. This is useful if the response was lost, and the candidate
	// sends another request.
	alreadyVotedForCandidate := r.votedFor == req.CandidateID

	// If the logs have last entries with different terms, the log with the
	// later term is more up-to-date.
	laterTerm := req.LastLogTerm > r.logTerm(r.storage.NumEntries())

	// If the logs end with the same term, whichever log is longer is more
	// up-to-date.
	longEnough := req.LastLogTerm == r.logTerm(r.storage.NumEntries()) && req.LastLogIndex >= r.storage.NumEntries()

	// We can only grant a vote if: we have not voted yet, we vote for the
	// same candidate again, or this is a pre-vote.
	canGrantVote := !voted || alreadyVotedForCandidate || req.PreVote

	// #RV2 If votedFor is null or candidateId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote.
	if canGrantVote && (laterTerm || longEnough) {
		if logLevel >= INFO {
			r.logger.to(req.CandidateID, fmt.Sprintf("%sVote granted for term %d", pre, req.Term))
		}

		if req.PreVote {
			return &pb.RequestVoteResponse{VoteGranted: true, Term: req.Term}
		}

		r.votedFor = req.CandidateID
		err := r.storage.Set(KeyVotedFor, req.CandidateID)

		if err != nil {
			panic(fmt.Errorf("couldn't save VotedFor: %v", err))
		}

		// #F2 If election timeout elapses without receiving
		// AppendEntries RPC from current leader or granting a vote to
		// candidate: convert to candidate. Here we are granting a vote
		// to a candidate so we reset the election timeout.
		r.baseline.Restart()
		r.election.Restart()
		r.heartbeat.Disable()
		r.cycle()

		return &pb.RequestVoteResponse{VoteGranted: true, Term: r.currentTerm}
	}

	// #RV2 The candidate's log was not up-to-date
	return &pb.RequestVoteResponse{Term: r.currentTerm}
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
	if req.Term < r.currentTerm {
		return &pb.AppendEntriesResponse{
			Success: false,
			Term:    req.Term,
		}
	}

	var prevEntry *pb.Entry

	// TODO Wrap storage with functions that panic.
	if req.PrevLogIndex > 0 && req.PrevLogIndex-1 < r.storage.NumEntries() {
		var err error
		prevEntry, err = r.storage.GetEntry(req.PrevLogIndex - 1)

		if err != nil {
			panic(fmt.Errorf("couldn't retrieve entry: %v", err))
		}
	}

	// TODO Refactor so it's understandable.
	success := req.PrevLogIndex == 0 || (req.PrevLogIndex-1 < r.storage.NumEntries() && prevEntry.Term == req.PrevLogTerm)

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if req.Term > r.currentTerm {
		r.becomeFollower(req.Term)
	} else if r.id != req.LeaderID {
		r.becomeFollower(r.currentTerm)
	}

	if success {
		r.leader = req.LeaderID
		r.heardFromLeader = true
		r.seenLeader = true

		lcd := req.PrevLogIndex + 1

		for _, entry := range req.Entries {
			if lcd == r.storage.NumEntries() || r.logTerm(lcd) != entry.Term {
				break
			}

		}

		err := r.storage.RemoveEntriesFrom(lcd - 1)

		if err != nil {
			panic(fmt.Errorf("couldn't remove excessive entries: %v", err))
		}

		toSave := req.Entries[req.PrevLogIndex-lcd+1:]

		err = r.storage.StoreEntries(toSave)

		if err != nil {
			panic(fmt.Errorf("couldn't save entries: %v", err))
		}

		if logLevel >= DEBUG {
			r.logger.to(req.LeaderID, fmt.Sprintf("AppendEntries persisted %d entries to stable storage", len(toSave)))
		}

		old := r.commitIndex

		r.commitIndex = min(req.CommitIndex, lcd)

		if r.commitIndex > old {
			r.newCommit(old)
		}
	}

	return &pb.AppendEntriesResponse{
		Term:       req.Term,
		MatchIndex: r.storage.NumEntries(),
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

		if old, ok := r.commands[commandID]; ok {
			response := make(chan *pb.ClientCommandRequest, 1)
			response <- old

			return response, true
		}

		r.Unlock()
		r.queue <- &pb.Entry{Term: r.currentTerm, Data: request}
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

	if r.state == Leader && r.logTerm(r.matchIndex) == r.currentTerm {
		r.commitIndex = max(r.commitIndex, r.matchIndex)
	}

	if r.commitIndex > old {
		r.newCommit(old)
	}
}

// TODO Assumes caller already holds lock on Replica
func (r *Replica) newCommit(old uint64) {
	for i := old; i < r.commitIndex; i++ {
		committed, err := r.storage.GetEntry(i)

		if err != nil {
			panic(fmt.Errorf("couldn't retrieve entry: %v", err))
		}

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

		r.commands[commandID] = committed.Data
	}
}

func (r *Replica) startElection() {
	r.Lock()
	defer r.Unlock()

	r.state = Candidate
	r.electionTimeout = randomTimeout(r.electionTimeout)

	term := r.currentTerm + 1
	pre := "pre"

	if !r.preElection {
		pre = ""
		// We are now a candidate. See Raft Paper Figure 2 -> Rules for Servers -> Candidates.
		// #C1 Increment currentTerm.
		r.currentTerm++

		err := r.storage.Set(KeyTerm, r.id)

		if err != nil {
			panic(fmt.Errorf("couldn't save Term: %v", err))
		}

		// #C2 Vote for self.
		r.votedFor = r.id

		err = r.storage.Set(KeyVotedFor, r.id)

		if err != nil {
			panic(fmt.Errorf("couldn't save VotedFor: %v", err))
		}
	}

	if logLevel >= INFO {
		r.logger.log(fmt.Sprintf("Starting %s election for term %d", pre, term))
	}

	// #C4 Send RequestVote RPCs to all other servers.
	r.rvreqout <- &pb.RequestVoteRequest{
		CandidateID:  r.id,
		Term:         term,
		LastLogTerm:  r.logTerm(r.storage.NumEntries()),
		LastLogIndex: r.storage.NumEntries(),
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

	term := r.currentTerm

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
			// Immediately start real election.
			r.election.Timeout()
			r.cycle()

			return
		}

		// We have received at least a quorum of votes.
		// We are the leader for this term. See Raft Paper Figure 2 -> Rules for Servers -> Leaders.
		if logLevel >= INFO {
			r.logger.log(fmt.Sprintf("Elected leader for term %d", r.currentTerm))
		}

		r.state = Leader
		r.leader = r.id
		r.seenLeader = true
		r.heardFromLeader = true
		r.nextIndex = r.storage.NumEntries() + 1

		// #L1 Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
		r.baseline.Disable()
		r.election.Disable()
		r.heartbeat.Restart()
		r.cycle()

		return
	}

	// #C7 If election timeout elapses: start new election.
	// This will happened if we don't receive enough replies in time. Or we lose the election but don't see a higher term number.
}

func (r *Replica) sendAppendEntries() {
	r.Lock()
	defer r.Unlock()

	if logLevel >= DEBUG {
		r.logger.log(fmt.Sprintf("Sending AppendEntries for term %d", r.currentTerm))
	}

	var toSave []*pb.Entry

LOOP:
	for i := r.maxAppendEntries; i > 0; i-- {
		select {
		case entry := <-r.queue:
			toSave = append(toSave, entry)
		default:
			break LOOP
		}
	}

	err := r.storage.StoreEntries(toSave)

	if err != nil {
		panic(fmt.Errorf("couldn't save entries: %v", err))
	}

	// #L1
	entries := []*pb.Entry{}

	next := r.nextIndex - 1

	if r.storage.NumEntries() > next {
		maxEntries := min(next+r.maxAppendEntries, r.storage.NumEntries())

		if !r.batch {
			maxEntries = next + 1
		}

		entries, err = r.storage.GetEntries(next, maxEntries)

		if err != nil {
			panic(fmt.Errorf("couldn't retrieve entries: %v", err))
		}
	}

	if logLevel >= DEBUG {
		r.logger.log(fmt.Sprintf("Sending %d entries", len(entries)))
	}

	r.aereqout <- &pb.AppendEntriesRequest{
		LeaderID:     r.id,
		Term:         r.currentTerm,
		PrevLogIndex: next,
		PrevLogTerm:  r.logTerm(next),
		CommitIndex:  r.commitIndex,
		Entries:      entries,
	}
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
	if response.Term > r.currentTerm {
		r.becomeFollower(response.Term)

		return
	}

	// Ignore late response
	if response.Term < r.currentTerm {
		return
	}

	if r.state == Leader {
		if response.Success {
			r.matchIndex = response.MatchIndex
			r.nextIndex = r.matchIndex + 1

			return
		}

		// If AppendEntries was not successful lower match index.
		r.nextIndex = max(1, response.MatchIndex)
	}
}

// TODO Tests.
func (r *Replica) becomeFollower(term uint64) {
	r.state = Follower
	r.preElection = true

	if r.currentTerm != term {
		if logLevel >= INFO {
			r.logger.log(fmt.Sprintf("Become follower as we transition from term %d to %d", r.currentTerm, term))
		}

		r.currentTerm = term
		r.votedFor = None

		err := r.storage.Set(KeyTerm, term)

		if err != nil {
			panic(fmt.Errorf("couldn't save Term: %v", err))
		}
		err = r.storage.Set(KeyVotedFor, None)

		if err != nil {
			panic(fmt.Errorf("couldn't save VotedFor: %v", err))
		}
	}

	// Reset election and baseline timeouts and disable heartbeat.
	r.baseline.Restart()
	r.election.Restart()
	r.heartbeat.Disable()
	r.cycle()
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

func (r *Replica) logTerm(index uint64) uint64 {
	if index < 1 || index > r.storage.NumEntries() {
		return 0
	}

	entry, err := r.storage.GetEntry(index - 1)

	if err != nil {
		panic(fmt.Errorf("couldn't retrieve entry: %v", err))
	}

	return entry.Term
}

// State returns the current raft state.
func (r *Replica) State() State {
	r.Lock()
	defer r.Unlock()

	return r.state
}

func (r *Replica) cycle() {
	r.cyclech <- struct{}{}
}
