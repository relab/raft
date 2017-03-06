package raftgorums

import (
	"container/list"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
	pb "github.com/relab/raft/raftgorums/raftpb"
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

// Config contains the configuration needed to start an instance of Raft.
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

// Raft represents an instance of the Raft algorithm.
type Raft struct {
	// Must be acquired before mutating Raft state.
	sync.Mutex

	id     uint64
	leader uint64

	currentTerm uint64
	votedFor    uint64

	sm raft.StateMachine

	storage Storage

	seenLeader      bool
	heardFromLeader bool

	state State

	addrs []string

	commitIndex uint64

	majorityNextIndex  uint64
	majorityMatchIndex uint64

	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	baselineTimeout  time.Duration
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	baseline  *Timer
	election  *Timer
	heartbeat *Timer

	cyclech chan struct{}

	preElection bool

	maxAppendEntries uint64
	queue            chan *raft.EntryFuture
	pending          *list.List

	pendingReads []*raft.EntryFuture

	applyCh   chan *entryFuture
	snapCh    chan chan<- *commonpb.Snapshot
	restoreCh chan *restoreFuture

	batch bool

	rvreqout chan *pb.RequestVoteRequest
	aereqout chan *pb.AppendEntriesRequest

	logger *Logger
}

type entryFuture struct {
	entry  *commonpb.Entry
	future *raft.EntryFuture
}

type restoreFuture struct {
	snapshot *commonpb.Snapshot
	done     chan struct{}
}

// NewRaft returns a new Raft given a configuration.
func NewRaft(sm raft.StateMachine, cfg *Config) *Raft {
	// TODO Validate config, i.e., make sure to sensible defaults if an
	// option is not configured.
	if cfg.Logger == nil {
		cfg.Logger = log.New(ioutil.Discard, "", 0)
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
	r := &Raft{
		id:                cfg.ID,
		currentTerm:       term,
		votedFor:          votedFor,
		sm:                sm,
		storage:           cfg.Storage,
		batch:             cfg.Batch,
		addrs:             cfg.Nodes,
		majorityNextIndex: 1,
		nextIndex:         make(map[uint64]uint64),
		matchIndex:        make(map[uint64]uint64),
		baselineTimeout:   cfg.ElectionTimeout,
		electionTimeout:   electionTimeout,
		heartbeatTimeout:  cfg.HeartbeatTimeout,
		baseline:          NewTimer(cfg.ElectionTimeout),
		election:          NewTimer(electionTimeout),
		heartbeat:         heartbeat,
		cyclech:           make(chan struct{}, 128),
		preElection:       true,
		maxAppendEntries:  cfg.MaxAppendEntries,
		queue:             make(chan *raft.EntryFuture, BufferSize),
		applyCh:           make(chan *entryFuture, 128),
		snapCh:            make(chan chan<- *commonpb.Snapshot),
		restoreCh:         make(chan *restoreFuture),
		rvreqout:          make(chan *pb.RequestVoteRequest, BufferSize),
		aereqout:          make(chan *pb.AppendEntriesRequest, BufferSize),
		logger:            &Logger{cfg.ID, cfg.Logger},
	}

	r.logger.Printf("ElectionTimeout: %v", r.electionTimeout)

	return r
}

// RequestVoteRequestChan returns a channel for outgoing RequestVoteRequests.
// It's the implementers responsibility to make sure these requests are
// delivered.
func (r *Raft) RequestVoteRequestChan() chan *pb.RequestVoteRequest {
	return r.rvreqout
}

// AppendEntriesRequestChan returns a channel for outgoing RequestVoteRequests.
// It's the implementers responsibility to make sure these requests are
// delivered.
func (r *Raft) AppendEntriesRequestChan() chan *pb.AppendEntriesRequest {
	return r.aereqout
}

// Run handles timeouts.
// All RPCs are handled by Gorums.
func (r *Raft) Run() {
	go r.runStateMachine()

	for {
		select {
		case <-r.baseline.C:
			r.Lock()
			r.heardFromLeader = false
			r.Unlock()
		case <-r.election.C:
			r.Lock()
			state := r.state
			r.Unlock()

			if state == Leader {
				// Thesis §6.2: A leader in Raft steps down if
				// an election timeout elapses without a
				// successful round of heartbeats to a majority
				// of its cluster.
				r.Lock()
				r.becomeFollower(r.currentTerm)
				r.Unlock()
			} else {
				// #F2 If election timeout elapses without
				// receiving AppendEntries RPC from current
				// leader or granting vote to candidate: convert
				// to candidate.
				r.startElection()
			}

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
func (r *Raft) HandleRequestVoteRequest(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
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
func (r *Raft) HandleAppendEntriesRequest(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
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

	var prevEntry *commonpb.Entry

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

		if len(toSave) > 0 {
			lcd = toSave[len(toSave)-1].Index
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

// ProposeConf implements raft.Raft.
func (r *Raft) ProposeConf(ctx context.Context, conf raft.TODOConfChange) error {
	panic("not implemented")
}

// ProposeCmd implements raft.Raft.
func (r *Raft) ProposeCmd(ctx context.Context, cmd []byte) (raft.Future, error) {
	future, err := r.cmdToFuture(cmd)

	if err != nil {
		return nil, err
	}

	select {
	case r.queue <- future:
		return future, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ReadCmd implements raft.Raft.
func (r *Raft) ReadCmd(ctx context.Context, cmd []byte) (raft.Future, error) {
	future, err := r.cmdToFuture(cmd)

	if err != nil {
		return nil, err
	}

	r.Lock()
	r.pendingReads = append(r.pendingReads, future)
	r.Unlock()

	return future, nil
}

func (r *Raft) cmdToFuture(cmd []byte) (*raft.EntryFuture, error) {
	r.Lock()
	state := r.state
	leader := r.leader
	term := r.currentTerm
	r.Unlock()

	if state != Leader {
		return nil, raft.ErrNotLeader{Leader: leader}
	}

	entry := &commonpb.Entry{
		EntryType: commonpb.EntryNormal,
		Term:      term,
		Data:      cmd,
	}

	return raft.NewFuture(entry), nil
}

func (r *Raft) advanceCommitIndex() {
	r.Lock()
	defer r.Unlock()

	if r.state != Leader {
		return
	}

	old := r.commitIndex

	if r.logTerm(r.majorityMatchIndex) == r.currentTerm {
		r.commitIndex = max(r.commitIndex, r.majorityMatchIndex)
	}

	if r.commitIndex > old {
		r.newCommit(old)
	}

	for _, future := range r.pendingReads {
		r.applyCh <- &entryFuture{future.Entry, future}
	}

	r.pendingReads = nil
}

// TODO Assumes caller already holds lock on Raft.
func (r *Raft) newCommit(old uint64) {
	// TODO Change to GetEntries -> then ring buffer.
	for i := old; i < r.commitIndex; i++ {
		switch r.state {
		case Leader:
			e := r.pending.Front()
			if e != nil {
				future := e.Value.(*raft.EntryFuture)
				r.applyCh <- &entryFuture{future.Entry, future}
				r.pending.Remove(e)
				break
			}
			fallthrough
		default:
			committed, err := r.storage.GetEntry(i)

			if err != nil {
				panic(fmt.Errorf("couldn't retrieve entry: %v", err))
			}

			r.applyCh <- &entryFuture{committed, nil}
		}
	}
}

func (r *Raft) runStateMachine() {
	apply := func(entry *commonpb.Entry, future *raft.EntryFuture) {
		var res interface{}
		if entry.EntryType != commonpb.EntryInternal {
			res = r.sm.Apply(entry)
		}

		if future != nil {
			future.Respond(res)
		}
	}

	for {
		select {
		case commit := <-r.applyCh:
			apply(commit.entry, commit.future)
		case future := <-r.snapCh:
			future <- r.sm.Snapshot()
		case future := <-r.restoreCh:
			r.sm.Restore(future.snapshot)
			close(future.done)
		}
	}
}

func (r *Raft) startElection() {
	r.Lock()
	defer r.Unlock()

	r.state = Candidate
	r.electionTimeout = randomTimeout(r.baselineTimeout)

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
func (r *Raft) HandleRequestVoteResponse(response *pb.RequestVoteResponse) {
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
		r.majorityNextIndex = r.storage.NumEntries() + 1
		r.pending = list.New()
		r.pendingReads = nil

		// Empty queue.
	EMPTYCH:
		for {
			select {
			case <-r.queue:
			default:
				// Paper §8: We add a no-op, so that the leader
				// commits an entry from its own term. This
				// ensures that the leader knows which entries
				// are committed.
				r.queue <- raft.NewFuture(&commonpb.Entry{
					EntryType: commonpb.EntryInternal,
					Term:      r.currentTerm,
					Data:      raft.NOOP,
				})
				break EMPTYCH
			}
		}

		// #L1 Upon election: send initial empty (no-op) AppendEntries
		// RPCs (heartbeat) to each server.
		r.baseline.Disable()
		r.election.Restart()
		r.heartbeat.Restart()
		r.cycle()

		return
	}

	r.preElection = true

	// #C7 If election timeout elapses: start new election.
	// This will happened if we don't receive enough replies in time. Or we lose the election but don't see a higher term number.
}

func (r *Raft) sendAppendEntries() {
	r.Lock()
	defer r.Unlock()

	if logLevel >= DEBUG {
		r.logger.log(fmt.Sprintf("Sending AppendEntries for term %d", r.currentTerm))
	}

	var toSave []*commonpb.Entry
	assignIndex := r.storage.NumEntries() + 1

LOOP:
	for i := r.maxAppendEntries; i > 0; i-- {
		select {
		case future := <-r.queue:
			future.Entry.Index = assignIndex
			assignIndex++
			toSave = append(toSave, future.Entry)
			r.pending.PushBack(future)
		default:
			break LOOP
		}
	}

	err := r.storage.StoreEntries(toSave)

	if err != nil {
		panic(fmt.Errorf("couldn't save entries: %v", err))
	}

	// #L1
	entries := r.getNextEntries(r.majorityNextIndex)

	if logLevel >= DEBUG {
		r.logger.log(fmt.Sprintf("Sending %d entries", len(entries)))
	}

	r.aereqout <- r.getAppendEntriesRequest(r.majorityNextIndex, entries)
}

// TODO Assumes caller holds lock on Raft.
func (r *Raft) getNextEntries(nextIndex uint64) []*commonpb.Entry {
	var entries []*commonpb.Entry

	next := nextIndex - 1

	if r.storage.NumEntries() > next {
		maxEntries := min(next+r.maxAppendEntries, r.storage.NumEntries())

		if !r.batch {
			maxEntries = next + 1
		}

		var err error
		entries, err = r.storage.GetEntries(next, maxEntries)

		if err != nil {
			panic(fmt.Errorf("couldn't retrieve entries: %v", err))
		}
	}

	return entries
}

// TODO Assumes caller holds lock on Raft.
func (r *Raft) getAppendEntriesRequest(nextIndex uint64, entries []*commonpb.Entry) *pb.AppendEntriesRequest {
	return &pb.AppendEntriesRequest{
		LeaderID:     r.id,
		Term:         r.currentTerm,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  r.logTerm(nextIndex - 1),
		CommitIndex:  r.commitIndex,
		Entries:      entries,
	}
}

// HandleAppendEntriesResponse must be invoked when receiving an
// AppendEntriesResponse.
func (r *Raft) HandleAppendEntriesResponse(response *pb.AppendEntriesResponse, responders int) {
	r.Lock()
	defer func() {
		r.Unlock()
		r.advanceCommitIndex()
	}()

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	// If we didn't get a response from a majority (excluding self) step down.
	if response.Term > r.currentTerm || responders < len(r.addrs)/2 {
		r.becomeFollower(response.Term)

		return
	}

	// Ignore late response
	if response.Term < r.currentTerm {
		return
	}

	if r.state == Leader {
		if response.Success {
			// Successful heartbeat to a majority.
			r.election.Restart()

			r.majorityMatchIndex = response.MatchIndex
			r.majorityNextIndex = r.majorityMatchIndex + 1

			return
		}

		// If AppendEntries was not successful lower match index.
		r.majorityNextIndex = max(1, response.MatchIndex)
	}
}

// TODO Tests.
// TODO Assumes caller already holds lock on Raft.
func (r *Raft) becomeFollower(term uint64) {
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

func (r *Raft) getHint() uint32 {
	r.Lock()
	defer r.Unlock()

	var hint uint32

	if r.seenLeader {
		hint = uint32(r.leader)
	}

	// If client receives hint = 0, it should try another random server.
	return hint
}

func (r *Raft) logTerm(index uint64) uint64 {
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
func (r *Raft) State() State {
	r.Lock()
	defer r.Unlock()

	return r.state
}

func (r *Raft) cycle() {
	r.cyclech <- struct{}{}
}
