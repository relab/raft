package raftgorums

import (
	"container/list"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Sirupsen/logrus"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
	gorums "github.com/relab/raft/raftgorums/gorumspb"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

// State represents one of the Raft server states.
type State int

// Server states.
const (
	Inactive State = iota
	Follower
	Candidate
	Leader
)

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

// Raft represents an instance of the Raft algorithm.
type Raft struct {
	// Must be acquired before mutating Raft state.
	sync.Mutex

	id     uint64
	leader uint64

	currentTerm uint64
	votedFor    uint64

	sm raft.StateMachine

	storage *raft.PanicStorage

	seenLeader      bool
	heardFromLeader bool

	state State

	mem *membership

	addrs []string

	lookup  map[uint64]int
	peers   []string
	cluster []uint64

	match map[uint32]chan uint64

	commitIndex  uint64
	appliedIndex uint64

	nextIndex  uint64
	matchIndex uint64

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	resetElection bool
	resetBaseline bool

	startElectionNow chan struct{}
	preElection      bool

	maxAppendEntries uint64
	queue            chan *raft.EntryFuture
	pending          *list.List

	pendingReads []*raft.EntryFuture

	applyCh chan *entryFuture

	batch bool

	rvreqout chan *pb.RequestVoteRequest
	aereqout chan *pb.AppendEntriesRequest
	cureqout chan *catchUpReq

	toggle chan struct{}

	logger logrus.FieldLogger

	metricsEnabled bool
}

type catchUpReq struct {
	leaderID   uint64
	matchIndex uint64
}

// TODO Just use future.
type entryFuture struct {
	entry  *commonpb.Entry
	future *raft.EntryFuture
}

// NewRaft returns a new Raft given a configuration.
func NewRaft(sm raft.StateMachine, cfg *Config) *Raft {
	// TODO Validate config, i.e., make sure to sensible defaults if an
	// option is not configured.
	storage := raft.NewPanicStorage(cfg.Storage, cfg.Logger)

	term := storage.Get(raft.KeyTerm)
	votedFor := storage.Get(raft.KeyVotedFor)

	// TODO Order.
	r := &Raft{
		id:               cfg.ID,
		currentTerm:      term,
		votedFor:         votedFor,
		sm:               sm,
		storage:          storage,
		batch:            cfg.Batch,
		addrs:            cfg.Servers,
		cluster:          cfg.InitialCluster,
		match:            make(map[uint32]chan uint64),
		nextIndex:        1,
		electionTimeout:  cfg.ElectionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		startElectionNow: make(chan struct{}),
		preElection:      true,
		maxAppendEntries: cfg.MaxAppendEntries,
		queue:            make(chan *raft.EntryFuture, BufferSize),
		applyCh:          make(chan *entryFuture, 128),
		rvreqout:         make(chan *pb.RequestVoteRequest, 128),
		aereqout:         make(chan *pb.AppendEntriesRequest, 128),
		cureqout:         make(chan *catchUpReq, 16),
		toggle:           make(chan struct{}, 1),
		logger:           cfg.Logger,
		metricsEnabled:   cfg.MetricsEnabled,
	}

	return r
}

// Run starts a server running the Raft algorithm.
func (r *Raft) Run(server *grpc.Server) error {
	addrs := make([]string, len(r.addrs))
	// We don't want to mutate r.addrs.
	copy(addrs, r.addrs)
	peers, lookup := initPeers(r.id, addrs)

	opts := []gorums.ManagerOption{
		gorums.WithGrpcDialOptions(
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(TCPConnect*time.Millisecond)),
	}

	mgr, err := gorums.NewManager(peers, opts...)

	if err != nil {
		return err
	}

	mem := &membership{
		id:     r.id,
		mgr:    mgr,
		lookup: lookup,
		logger: r.logger,
	}

	gorums.RegisterRaftServer(server, r)

	var clusterIDs []uint32

	for _, id := range r.cluster {
		if r.id == id {
			// Exclude self.
			r.state = Follower
			mem.enabled = true
			continue
		}
		r.logger.WithField("serverid", id).Warnln("Added to cluster")
		clusterIDs = append(clusterIDs, mem.getNodeID(id))
	}

	conf, err := mgr.NewConfiguration(clusterIDs, NewQuorumSpec(len(clusterIDs)+1))

	if err != nil {
		return err
	}

	mem.latest = conf
	mem.committed = conf
	r.mem = mem

	for _, nodeID := range mgr.NodeIDs() {
		r.match[nodeID] = make(chan uint64, 1)
	}

	go r.run()

	return r.handleOutgoing()
}

func initPeers(self uint64, addrs []string) ([]string, map[uint64]int) {
	// Exclude self.
	peers := append(addrs[:self-1], addrs[self:]...)

	var pos int
	lookup := make(map[uint64]int)

	for i := 1; i <= len(addrs); i++ {
		if uint64(i) == self {
			continue
		}

		lookup[uint64(i)] = pos
		pos++
	}

	return peers, lookup
}

func (r *Raft) run() {
	go r.runStateMachine()

	for {
		switch r.state {
		case Inactive:
			r.runDormant()
		default:
			r.runNormal()
		}

		if r.mem.isActive() {
			r.logger.Warnln("Dormant -> Normal")
			r.state = Follower
		} else {
			r.logger.Warnln("Normal -> Dormant")
			r.state = Inactive
		}

		r.becomeFollower(r.currentTerm)
	}
}

// runDormant runs Raft in a dormant state where it only accepts incoming
// requests and never times out. The server is able to receive AppendEntries
// from a leader and replicate log entries. If the server receives a
// configuration in which it is part of, it will transition to running the Run
// method.
func (r *Raft) runDormant() {
	baseline := func() {
		r.Lock()
		defer r.Unlock()
		if r.resetBaseline {
			r.resetBaseline = false
			return
		}
		r.heardFromLeader = false
	}

	baselineTimeout := time.After(r.electionTimeout)

	for {
		select {
		case <-baselineTimeout:
			baselineTimeout = time.After(r.electionTimeout)
			baseline()
		case <-r.toggle:
			return
		}
	}
}

// runNormal handles timeouts.
// All RPCs are handled by Gorums.
func (r *Raft) runNormal() {
	startElection := func() {
		r.Lock()
		defer r.Unlock()
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

		// #F2 If election timeout elapses without
		// receiving AppendEntries RPC from current
		// leader or granting vote to candidate: convert
		// to candidate.
		r.startElection()
	}

	baseline := func() {
		r.Lock()
		defer r.Unlock()
		if r.state == Leader {
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
		case <-r.toggle:
			return
		}
	}
}

func (r *Raft) cmdToFuture(cmd []byte, kind commonpb.EntryType) (*raft.EntryFuture, error) {
	r.Lock()
	state := r.state
	leader := r.leader
	term := r.currentTerm
	r.Unlock()

	if state != Leader {
		return nil, raft.ErrNotLeader{Leader: leader}
	}

	entry := &commonpb.Entry{
		EntryType: kind,
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
		r.mem.setStable(true)
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
	for i := old + 1; i <= r.commitIndex; i++ {
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
				if future.Entry.Index == i {
					r.applyCh <- &entryFuture{future.Entry, future}
					r.pending.Remove(e)
					break
				}
			}
			fallthrough
		default:
			committed := r.storage.GetEntry(i)
			if committed.Index != i {
				panic("entry tried applied out of order")
			}
			r.applyCh <- &entryFuture{committed, nil}
		}
	}
}

func (r *Raft) runStateMachine() {
	apply := func(commit *entryFuture) {
		var res interface{}

		switch commit.entry.EntryType {
		case commonpb.EntryInternal:
		case commonpb.EntryNormal:
			res = r.sm.Apply(commit.entry)
		case commonpb.EntryConfChange:
			// TODO We should be able to skip the unmarshaling if we
			// are not recovering.
			var reconf commonpb.ReconfRequest
			err := reconf.Unmarshal(commit.entry.Data)

			if err != nil {
				panic("could not unmarshal reconf")
			}

			r.mem.setPending(&reconf)
			r.mem.set(commit.entry.Index)
			// TODO Send to state machine.
			r.logger.Warnln("Comitted configuration")

			enabled := r.mem.commit()

			// Toggle if we need to change run routine.
			if (enabled && r.state == Inactive) || !enabled {
				select {
				case r.toggle <- struct{}{}:
				default:
				}
			}

			res = &commonpb.ReconfResponse{
				Status: commonpb.ReconfOK,
			}
		}

		if commit.future != nil {
			commit.future.Respond(res)
			if r.metricsEnabled {
				rmetrics.cmdCommit.Observe(time.Since(commit.future.Created).Seconds())
			}
		}
	}

	for {
		select {
		case commit := <-r.applyCh:
			apply(commit)
		}
	}
}

// TODO Assumes caller holds lock on Raft.
func (r *Raft) startElection() {
	r.state = Candidate
	term := r.currentTerm + 1

	if !r.preElection {
		// We are now a candidate. See Raft Paper Figure 2 -> Rules for Servers -> Candidates.
		// #C1 Increment currentTerm.
		r.currentTerm++
		r.storage.Set(raft.KeyTerm, r.currentTerm)

		// #C2 Vote for self.
		r.votedFor = r.id
		r.storage.Set(raft.KeyVotedFor, r.id)
	}

	r.logger.WithFields(logrus.Fields{
		"currentterm": r.currentTerm,
		"preelection": r.preElection,
	}).Infoln("Started election")

	lastLogIndex := r.storage.NextIndex() - 1
	lastLogTerm := r.logTerm(lastLogIndex)

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

func (r *Raft) sendAppendEntries() {
	r.Lock()
	defer r.Unlock()

	var toSave []*commonpb.Entry
	assignIndex := r.storage.NextIndex()

	// TODO This means the first entry in the log cannot be a configuration
	// change. This should not pose a problem as we always commit a no-op
	// first. This needs to be dealt with if we decide to store the initial
	// configuration at the first index however.
	var reconf uint64

LOOP:
	for i := r.maxAppendEntries; i > 0; i-- {
		select {
		case future := <-r.queue:
			future.Entry.Index = assignIndex
			if future.Entry.EntryType == commonpb.EntryConfChange {
				reconf = assignIndex
			}
			assignIndex++
			toSave = append(toSave, future.Entry)
			r.pending.PushBack(future)
		default:
			break LOOP
		}
	}

	if len(toSave) > 0 {
		r.storage.StoreEntries(toSave)
	}

	if reconf > 0 {
		r.mem.set(reconf)
	}

	r.aereqout <- r.getAppendEntriesRequest(r.nextIndex, nil)
}

// TODO Assumes caller holds lock on Raft.
func (r *Raft) getNextEntries(nextIndex uint64) []*commonpb.Entry {
	var entries []*commonpb.Entry

	next := nextIndex
	logLen := r.storage.NextIndex() - 1

	if next <= logLen {
		maxEntries := min(next+r.maxAppendEntries, logLen)

		if !r.batch {
			// One entry at the time.
			maxEntries = next
		}

		entries = r.storage.GetEntries(next, maxEntries)
	}

	return entries
}

// TODO Assumes caller holds lock on Raft.
func (r *Raft) getAppendEntriesRequest(nextIndex uint64, entries []*commonpb.Entry) *pb.AppendEntriesRequest {
	prevIndex := nextIndex - 1
	prevTerm := r.logTerm(prevIndex)

	return &pb.AppendEntriesRequest{
		LeaderID:     r.id,
		Term:         r.currentTerm,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		CommitIndex:  r.commitIndex,
		Entries:      entries,
	}
}

// TODO Tests.
// TODO Assumes caller already holds lock on Raft.
func (r *Raft) becomeFollower(term uint64) {
	if r.state != Inactive {
		r.state = Follower
	}
	r.preElection = true

	if r.currentTerm != term {
		r.logger.WithFields(logrus.Fields{
			"currentterm": term,
			"oldterm":     r.currentTerm,
		}).Infoln("Transition to follower")

		r.currentTerm = term
		r.votedFor = None

		r.storage.Set(raft.KeyTerm, term)
		r.storage.Set(raft.KeyVotedFor, None)
	}

	// Reset election and baseline timeouts.
	r.resetBaseline = true
	r.resetElection = true
}

func (r *Raft) logTerm(index uint64) uint64 {
	if index < 1 || index > r.storage.NextIndex()-1 {
		return 0
	}

	entry := r.storage.GetEntry(index)
	return entry.Term
}

// State returns the current raft state.
func (r *Raft) State() State {
	r.Lock()
	defer r.Unlock()

	return r.state
}
