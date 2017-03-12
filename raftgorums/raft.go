package raftgorums

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

// LogLevel sets the level of logging.
const LogLevel = logrus.InfoLevel

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

	storage *panicStorage

	seenLeader      bool
	heardFromLeader bool

	state State

	addrs []string

	commitIndex  uint64
	appliedIndex uint64

	nextIndex  uint64
	matchIndex uint64

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	skipElection  bool
	resetElection bool
	resetBaseline bool

	startElectionNow chan struct{}
	preElection      bool

	maxAppendEntries uint64
	queue            chan *raft.EntryFuture
	pending          *list.List

	pendingReads []*raft.EntryFuture

	applyCh   chan *entryFuture
	restoreCh chan *commonpb.Snapshot

	currentSnapshot *commonpb.Snapshot

	batch bool

	rvreqout chan *pb.RequestVoteRequest
	aereqout chan *pb.AppendEntriesRequest

	logger logrus.FieldLogger

	metricsEnabled bool
}

type entryFuture struct {
	entry  *commonpb.Entry
	future *raft.EntryFuture
}

// NewRaft returns a new Raft given a configuration.
func NewRaft(sm raft.StateMachine, cfg *Config) *Raft {
	// TODO Validate config, i.e., make sure to sensible defaults if an
	// option is not configured.
	storage := &panicStorage{cfg.Storage, cfg.Logger}

	term := storage.Get(KeyTerm)
	votedFor := storage.Get(KeyVotedFor)

	// TODO Order.
	r := &Raft{
		id:               cfg.ID,
		currentTerm:      term,
		votedFor:         votedFor,
		sm:               sm,
		storage:          storage,
		batch:            cfg.Batch,
		addrs:            cfg.Nodes,
		nextIndex:        1,
		electionTimeout:  cfg.ElectionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		startElectionNow: make(chan struct{}),
		preElection:      true,
		maxAppendEntries: cfg.MaxAppendEntries,
		queue:            make(chan *raft.EntryFuture, BufferSize),
		applyCh:          make(chan *entryFuture, 128),
		restoreCh:        make(chan *commonpb.Snapshot, 1),
		rvreqout:         make(chan *pb.RequestVoteRequest, 128),
		aereqout:         make(chan *pb.AppendEntriesRequest, 128),
		logger:           cfg.Logger,
		metricsEnabled:   cfg.MetricsEnabled,
	}

	snapshot, err := cfg.Storage.GetSnapshot()

	if err != nil {
		r.setSnapshot(&commonpb.Snapshot{})
		return r
	}

	// Restore state machine if snapshot exists.
	r.setSnapshot(snapshot)
	r.restoreFromSnapshot()

	return r
}

// Run handles timeouts.
// All RPCs are handled by Gorums.
func (r *Raft) Run() {
	go r.runStateMachine()

	startElection := func() {
		r.Lock()
		defer r.Unlock()
		if r.skipElection {
			return
		}
		if r.resetElection {
			r.resetElection = false
			return
		}

		if r.state == Leader {
			r.logger.Warnln("Leader stepping down")
			// Thesis §6.2: A leader in Raft steps down if
			// an election timeout elapses without a
			// successful round of heartbeats to a majority
			// of its cluster.
			r.becomeFollower(r.currentTerm)
			return
		}

		r.logger.Warnln("Starting election")
		// #F2 If election timeout elapses without
		// receiving AppendEntries RPC from current
		// leader or granting vote to candidate: convert
		// to candidate.
		r.startElection()
	}

	baseline := func() {
		r.Lock()
		defer r.Unlock()
		if r.skipElection || r.state == Leader {
			return
		}
		if r.resetBaseline {
			r.resetBaseline = false
			return
		}
		r.heardFromLeader = false
	}

	baselineTimeout := time.After(r.electionTimeout)
	rndTimeout := randomTimeout(r.electionTimeout)
	electionTimeout := time.After(rndTimeout)
	heartbeatTimeout := time.After(r.heartbeatTimeout)

	r.logger.WithField("electiontimeout", rndTimeout).
		Infoln("Set election timeout")

	for {
		select {
		case <-baselineTimeout:
			baselineTimeout = time.After(r.electionTimeout)
			baseline()
		case <-electionTimeout:
			rndTimeout := randomTimeout(r.electionTimeout)
			electionTimeout = time.After(rndTimeout)

			r.logger.WithField("electiontimeout", rndTimeout).
				Infoln("Set election timeout")

			startElection()
		case <-r.startElectionNow:
			rndTimeout := randomTimeout(r.electionTimeout)
			electionTimeout = time.After(rndTimeout)

			r.logger.WithField("electiontimeout", rndTimeout).
				Infoln("Set election timeout")

			startElection()
		case <-heartbeatTimeout:
			heartbeatTimeout = time.After(r.heartbeatTimeout)
			if r.State() != Leader {
				continue
			}
			r.sendAppendEntries()
		}
	}
}

// HandleRequestVoteRequest must be called when receiving a RequestVoteRequest,
// the return value must be delivered to the requester.
func (r *Raft) HandleRequestVoteRequest(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	r.Lock()
	defer r.Unlock()
	if r.metricsEnabled {
		timer := metrics.NewTimer(rmetrics.rvreq)
		defer timer.ObserveDuration()
	}

	var voteGranted bool
	defer func() {
		r.logger.WithFields(logrus.Fields{
			"currentterm": r.currentTerm,
			"requestterm": req.Term,
			"prevote":     req.PreVote,
			"candidateid": req.CandidateID,
			"votegranted": voteGranted,
		}).Infoln("Got vote request")
	}()

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

	logLen := r.storage.NextIndex()
	lastLogTerm := r.logTerm(logLen)

	// We can grant a vote in the same term, as long as it's to the same
	// candidate. This is useful if the response was lost, and the candidate
	// sends another request.
	alreadyVotedForCandidate := r.votedFor == req.CandidateID

	// If the logs have last entries with different terms, the log with the
	// later term is more up-to-date.
	laterTerm := req.LastLogTerm > lastLogTerm

	// If the logs end with the same term, whichever log is longer is more
	// up-to-date.
	longEnough := req.LastLogTerm == lastLogTerm && req.LastLogIndex >= logLen

	// We can only grant a vote if: we have not voted yet, we vote for the
	// same candidate again, or this is a pre-vote.
	canGrantVote := !voted || alreadyVotedForCandidate || req.PreVote

	// #RV2 If votedFor is null or candidateId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote.
	voteGranted = canGrantVote && (laterTerm || longEnough)

	if voteGranted {
		if req.PreVote {
			return &pb.RequestVoteResponse{VoteGranted: true, Term: req.Term}
		}

		r.votedFor = req.CandidateID
		r.storage.Set(KeyVotedFor, req.CandidateID)

		// #F2 If election timeout elapses without receiving
		// AppendEntries RPC from current leader or granting a vote to
		// candidate: convert to candidate. Here we are granting a vote
		// to a candidate so we reset the election timeout.
		r.resetElection = true
		r.resetBaseline = true

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
	if r.metricsEnabled {
		timer := metrics.NewTimer(rmetrics.aereq)
		defer timer.ObserveDuration()
	}

	reqLogger := r.logger.WithFields(logrus.Fields{
		"currentterm":  r.currentTerm,
		"requestterm":  req.Term,
		"leaderid":     req.LeaderID,
		"prevlogindex": req.PrevLogIndex,
		"commitindex":  req.CommitIndex,
		"lenentries":   len(req.Entries),
	})
	reqLogger.Infoln("Got AppendEntries")

	// #AE1 Reply false if term < currentTerm.
	if req.Term < r.currentTerm {
		return &pb.AppendEntriesResponse{
			Success: false,
			Term:    r.currentTerm,
		}
	}

	var prevTerm uint64
	logLen := r.storage.NextIndex()

	switch {
	case req.PrevLogIndex <= r.currentSnapshot.LastIncludedIndex:
		prevTerm = r.currentSnapshot.LastIncludedTerm
	case req.PrevLogIndex > 0 && req.PrevLogIndex-1 < logLen:
		prevEntry := r.storage.GetEntry(req.PrevLogIndex - 1)
		prevTerm = prevEntry.Term
	}

	// TODO Refactor so it's understandable.
	success := req.PrevLogIndex == 0 || (req.PrevLogIndex-1 < logLen && prevTerm == req.PrevLogTerm)

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if req.Term > r.currentTerm {
		r.becomeFollower(req.Term)
	} else if r.id != req.LeaderID {
		r.becomeFollower(r.currentTerm)
	}

	if r.metricsEnabled {
		rmetrics.leader.Set(float64(req.LeaderID))
	}

	// We acknowledge this server as the leader even if the append entries
	// request might be unsuccessful.
	r.leader = req.LeaderID
	r.heardFromLeader = true
	r.seenLeader = true

	if success {
		var toSave []*commonpb.Entry
		index := req.PrevLogIndex

		for _, entry := range req.Entries {
			index++

			if entry.Term != r.logTerm(index) {
				for r.storage.NextIndex() > index-1 {
					logLen = r.storage.NextIndex()
					r.storage.RemoveEntries(logLen-1, logLen-1)
				}
				toSave = append(toSave, entry)
			}
		}

		r.storage.StoreEntries(toSave)

		reqLogger.WithFields(logrus.Fields{
			"lensaved": len(toSave),
			"lenlog":   r.storage.NextIndex(),
		}).Infoln("Saved entries to stable storage")

		old := r.commitIndex
		r.commitIndex = min(req.CommitIndex, r.storage.NextIndex())

		if r.metricsEnabled {
			rmetrics.commitIndex.Set(float64(r.commitIndex))
		}

		r.logger.WithFields(logrus.Fields{
			"oldcommitindex": old,
			"commitindex":    r.commitIndex,
		}).Infoln("Set commit index")

		if r.commitIndex > old {
			r.newCommit(old)
		}
	}

	return &pb.AppendEntriesResponse{
		Term:       req.Term,
		MatchIndex: r.storage.NextIndex(),
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
	future, err := r.cmdToFuture(cmd)

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

	if r.logTerm(r.matchIndex) == r.currentTerm {
		r.commitIndex = max(r.commitIndex, r.matchIndex)
	}

	if r.commitIndex > old {
		if r.metricsEnabled {
			rmetrics.commitIndex.Set(float64(r.commitIndex))
		}

		r.logger.WithFields(logrus.Fields{
			"oldcommitindex": old,
			"commitindex":    r.commitIndex,
		}).Infoln("Set commit index")

		r.newCommit(old)
	}

	for _, future := range r.pendingReads {
		r.applyCh <- &entryFuture{future.Entry, future}
		rmetrics.reads.Add(1)
	}

	r.pendingReads = nil
}

// TODO Assumes caller already holds lock on Raft.
func (r *Raft) newCommit(old uint64) {
	// TODO Change to GetEntries -> then ring buffer.
	for i := old; i < r.commitIndex; i++ {
		if i < r.appliedIndex {
			r.logger.WithField("index", i).Warningln("Already applied")
			continue
		}

		r.appliedIndex = i

		switch r.state {
		case Leader:
			if r.metricsEnabled {
				rmetrics.writes.Add(1)
			}

			e := r.pending.Front()
			if e != nil {
				future := e.Value.(*raft.EntryFuture)
				r.applyCh <- &entryFuture{future.Entry, future}
				r.pending.Remove(e)
				break
			}
			fallthrough
		default:
			committed := r.storage.GetEntry(i)
			r.applyCh <- &entryFuture{committed, nil}
		}
	}
}

func (r *Raft) runStateMachine() {
	apply := func(commit *entryFuture) {
		var res interface{}
		if commit.entry.EntryType != commonpb.EntryInternal {
			res = r.sm.Apply(commit.entry)
		}

		if commit.future != nil {
			commit.future.Respond(res)
			if r.metricsEnabled {
				rmetrics.cmdCommit.Observe(time.Since(commit.future.Created).Seconds())
			}
		}
	}

	restore := func(snapshot *commonpb.Snapshot) {
		r.Lock()
		defer r.Unlock()
		snapLogger := r.logger.WithFields(logrus.Fields{
			"currentterm":       r.currentTerm,
			"lastincludedindex": snapshot.LastIncludedIndex,
			"lastincludedterm":  snapshot.LastIncludedTerm,
		})
		snapLogger.Infoln("Received snapshot")

		// Disable baseline/election timeout as we are not handling
		// append entries while applying the snapshot.
		r.skipElection = true
		defer func() {
			r.skipElection = false
		}()

		snapLogger.Infoln("Restored statemachine")

		r.setSnapshot(snapshot)
		r.restoreFromSnapshot()
	}

	snapCh := r.sm.Snapshot()

	for {
		select {
		case commit := <-r.applyCh:
			apply(commit)
		case snapshot := <-r.restoreCh:
			restore(snapshot)
		case snapshot := <-snapCh:
			r.Lock()
			r.setSnapshot(snapshot)
			r.Unlock()
		}
	}
}

// TODO Assumes caller holds lock on Raft.
func (r *Raft) setSnapshot(snapshot *commonpb.Snapshot) {
	start := time.Now()
	defer func() {
		fmt.Println("Writing snapshot to disk took:", time.Now().Sub(start))
	}()

	r.storage.SetSnapshot(snapshot)

	// TODO Clean log.

	r.currentSnapshot = snapshot
}

// TODO Assumes caller holds lock on Raft.
// Call after snapshot has been saved to disk.
func (r *Raft) restoreFromSnapshot() {
	snapshot := r.currentSnapshot

	old := r.commitIndex

	r.sm.Restore(snapshot)
	// TODO Make sure to not go back on commit index, use max().
	r.commitIndex = snapshot.LastIncludedIndex
	r.appliedIndex = snapshot.LastIncludedIndex
	r.nextIndex = r.currentSnapshot.LastIncludedIndex + 1
	r.becomeFollower(snapshot.LastIncludedTerm)

	if r.metricsEnabled {
		rmetrics.commitIndex.Set(float64(r.commitIndex))
	}

	r.logger.WithFields(logrus.Fields{
		"oldcommitindex": old,
		"commitindex":    r.commitIndex,
	}).Infoln("Set commit index")
}

// TODO Assumes caller holds lock on Raft.
func (r *Raft) startElection() {
	r.state = Candidate
	term := r.currentTerm + 1

	if !r.preElection {
		// We are now a candidate. See Raft Paper Figure 2 -> Rules for Servers -> Candidates.
		// #C1 Increment currentTerm.
		r.currentTerm++
		r.storage.Set(KeyTerm, r.id)

		// #C2 Vote for self.
		r.votedFor = r.id
		r.storage.Set(KeyVotedFor, r.id)
	}

	r.logger.WithFields(logrus.Fields{
		"currentterm": r.currentTerm,
	}).Infoln("Started election")

	lastLogIndex := r.storage.NextIndex()
	var lastLogTerm uint64

	if lastLogIndex == r.currentSnapshot.LastIncludedIndex {
		lastLogIndex = r.currentSnapshot.LastIncludedIndex
		lastLogTerm = r.currentSnapshot.LastIncludedTerm
	} else {
		lastLogTerm = r.logTerm(lastLogIndex)
	}

	// #C4 Send RequestVote RPCs to all other servers.
	r.rvreqout <- &pb.RequestVoteRequest{
		CandidateID:  r.id,
		Term:         term,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
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
	if r.metricsEnabled {
		timer := metrics.NewTimer(rmetrics.rvres)
		defer timer.ObserveDuration()
	}

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
			r.startElectionNow <- struct{}{}
			return
		}

		// We have received at least a quorum of votes.
		// We are the leader for this term. See Raft Paper Figure 2 -> Rules for Servers -> Leaders.

		if r.metricsEnabled {
			rmetrics.leader.Set(float64(r.id))
		}

		r.logger.WithFields(logrus.Fields{
			"currentterm": r.currentTerm,
		}).Infoln("Elected leader")

		logLen := r.storage.NextIndex()

		r.state = Leader
		r.leader = r.id
		r.seenLeader = true
		r.heardFromLeader = true
		r.nextIndex = logLen + 1
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

		// TODO r.sendAppendEntries()?

		return
	}

	r.preElection = true

	// #C7 If election timeout elapses: start new election.
	// This will happened if we don't receive enough replies in time. Or we lose the election but don't see a higher term number.
}

func (r *Raft) sendAppendEntries() {
	r.Lock()
	defer r.Unlock()

	var toSave []*commonpb.Entry
	logLen := r.storage.NextIndex()
	assignIndex := logLen + 1

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

	r.storage.StoreEntries(toSave)

	// #L1
	entries := r.getNextEntries(r.nextIndex)

	r.logger.WithFields(logrus.Fields{
		"currentterm": r.currentTerm,
		"lenentries":  len(entries),
	}).Infoln("Sending AppendEntries")

	r.aereqout <- r.getAppendEntriesRequest(r.nextIndex, entries)
}

// TODO Assumes caller holds lock on Raft.
func (r *Raft) getNextEntries(nextIndex uint64) []*commonpb.Entry {
	var entries []*commonpb.Entry

	next := nextIndex - 1
	logLen := r.storage.NextIndex()

	if logLen > next {
		maxEntries := min(next+r.maxAppendEntries, logLen)

		if !r.batch {
			// This is ok since GetEntries is exclusive of the last
			// index, meaning maxEntries == logLen won't be
			// out-of-bounds.
			maxEntries = next + 1
		}

		entries = r.storage.GetEntries(next, maxEntries)
	}

	return entries
}

// TODO Assumes caller holds lock on Raft.
func (r *Raft) getAppendEntriesRequest(nextIndex uint64, entries []*commonpb.Entry) *pb.AppendEntriesRequest {
	var prevTerm, prevIndex uint64 = 0, nextIndex - 1

	if prevIndex == r.currentSnapshot.LastIncludedIndex {
		prevIndex = r.currentSnapshot.LastIncludedIndex
		prevTerm = r.currentSnapshot.LastIncludedTerm
	} else {
		prevTerm = r.logTerm(prevIndex)
	}

	return &pb.AppendEntriesRequest{
		LeaderID:     r.id,
		Term:         r.currentTerm,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
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
	if r.metricsEnabled {
		timer := metrics.NewTimer(rmetrics.aeres)
		defer timer.ObserveDuration()
	}

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
			r.resetElection = true

			r.matchIndex = response.MatchIndex
			r.nextIndex = r.matchIndex + 1

			return
		}

		// If AppendEntries was not successful lower match index.
		r.nextIndex = max(1, response.MatchIndex)
	}
}

// TODO Tests.
// TODO Assumes caller already holds lock on Raft.
func (r *Raft) becomeFollower(term uint64) {
	r.state = Follower
	r.preElection = true

	if r.currentTerm != term {
		r.logger.WithFields(logrus.Fields{
			"currentterm": term,
			"oldterm":     r.currentTerm,
		}).Infoln("Transition to follower")

		r.currentTerm = term
		r.votedFor = None

		r.storage.Set(KeyTerm, term)
		r.storage.Set(KeyVotedFor, None)
	}

	// Reset election and baseline timeouts.
	r.resetBaseline = true
	r.resetElection = true
}

func (r *Raft) logTerm(index uint64) uint64 {
	if index < 1 || index > r.storage.NextIndex() {
		return 0
	}

	entry := r.storage.GetEntry(index - 1)
	return entry.Term
}

// State returns the current raft state.
func (r *Raft) State() State {
	r.Lock()
	defer r.Unlock()

	return r.state
}
