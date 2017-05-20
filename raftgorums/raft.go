package raftgorums

import (
	"container/list"
	"fmt"
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
	TCPConnect   = 50000
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
	mu sync.Mutex

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

	entriesPerMsg uint64
	burst         uint64
	maxInflight   int
	inflight      int

	heartbeatNow chan struct{}
	countLock    sync.Mutex
	cmdCount     uint64
	queue        chan raft.PromiseEntry
	pending      *list.List

	pendingReads []raft.PromiseLogEntry

	applyCh chan raft.PromiseLogEntry

	batch bool

	rvreqout chan *pb.RequestVoteRequest
	aereqout chan *pb.AppendEntriesRequest
	cureqout chan *catchUpReq

	toggle chan struct{}

	logger logrus.FieldLogger

	metricsEnabled bool

	stop chan struct{}

	lat   *raft.Latency
	event *raft.Event
}

func (r *Raft) incCmd() {
	r.countLock.Lock()
	defer r.countLock.Unlock()

	r.cmdCount++

	if r.cmdCount > r.entriesPerMsg {
		r.cmdCount = 0

		select {
		case r.heartbeatNow <- struct{}{}:
		default:
		}
	}
}

type catchUpReq struct {
	leaderID   uint64
	matchIndex uint64
}

// NewRaft returns a new Raft given a configuration.
func NewRaft(sm raft.StateMachine, cfg *Config, lat *raft.Latency, event *raft.Event) *Raft {
	err := validate(cfg)

	// TODO Make NewRaft return error.
	if err != nil {
		panic(err)
	}

	storage := raft.NewPanicStorage(cfg.Storage, cfg.Logger)

	term := storage.Get(raft.KeyTerm)
	votedFor := storage.Get(raft.KeyVotedFor)

	// TODO Order.
	r := &Raft{
		id:          cfg.ID,
		currentTerm: term,
		votedFor:    votedFor,
		sm:          sm,
		storage:     storage,
		mem: &membership{
			id:     cfg.ID,
			logger: cfg.Logger,
		},
		batch:            cfg.Batch,
		addrs:            cfg.Servers,
		cluster:          cfg.InitialCluster,
		match:            make(map[uint32]chan uint64),
		nextIndex:        1,
		electionTimeout:  cfg.ElectionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		startElectionNow: make(chan struct{}),
		heartbeatNow:     make(chan struct{}, 128),
		preElection:      true,
		entriesPerMsg:    cfg.EntriesPerMsg,
		maxInflight:      len(cfg.Servers) - 1,
		burst:            cfg.EntriesPerMsg * cfg.CatchupMultiplier,
		queue:            make(chan raft.PromiseEntry, BufferSize),
		applyCh:          make(chan raft.PromiseLogEntry, 1024),
		rvreqout:         make(chan *pb.RequestVoteRequest, 1024),
		aereqout:         make(chan *pb.AppendEntriesRequest, 1024),
		cureqout:         make(chan *catchUpReq, 16),
		toggle:           make(chan struct{}),
		logger:           cfg.Logger.WithField("raftid", cfg.ID),
		metricsEnabled:   cfg.MetricsEnabled,
		stop:             make(chan struct{}),
		lat:              lat,
		event:            event,
	}

	return r
}

// Stop forcibly stops the Raft server.
func (r *Raft) Stop() {
	close(r.stop)
	t := time.AfterFunc(time.Second, func() {
		// Panic if Raft is locked on Stop.
		panic(fmt.Sprintf("failed to aqcuire lock: %d", r.id))
	})
	r.mu.Lock()
	r.mu.Unlock()
	t.Stop()
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

	r.mem.mgr = mgr
	r.mem.lookup = lookup

	gorums.RegisterRaftServer(server, r)

	var clusterIDs []uint32

	for _, id := range r.cluster {
		if r.id == id {
			// Exclude self.
			r.state = Follower
			r.mem.enabled = true
			continue
		}
		r.logger.WithField("serverid", id).Warnln("Added to cluster")
		clusterIDs = append(clusterIDs, r.mem.getNodeID(id))
	}

	conf, err := mgr.NewConfiguration(clusterIDs, NewQuorumSpec(len(clusterIDs)+1))

	if err != nil {
		return err
	}

	r.mem.latest = conf
	r.mem.committed = conf

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

		select {
		case <-r.stop:
			return
		default:
		}

		if r.mem.isActive() {
			r.logger.Warnln("Now running in Normal mode")
			r.mu.Lock()
			r.state = Follower
			r.mu.Unlock()
		} else {
			r.logger.Warnln("Now running in Dormant mode")
			r.mu.Lock()
			r.state = Inactive
			r.mu.Unlock()
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
		r.mu.Lock()
		defer r.mu.Unlock()
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
		case <-r.stop:
			return
		}
	}
}

// runNormal handles timeouts.
// All RPCs are handled by Gorums.
func (r *Raft) runNormal() {
	startElection := func() {
		r.mu.Lock()
		defer r.mu.Unlock()
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
		r.mu.Lock()
		defer r.mu.Unlock()
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
		case <-r.heartbeatNow:
			heartbeatTimeout = time.After(r.heartbeatTimeout)
			if r.State() != Leader {
				continue
			}
			r.sendAppendEntries()
		case <-heartbeatTimeout:
			heartbeatTimeout = time.After(r.heartbeatTimeout)
			if r.State() != Leader {
				continue
			}
			r.sendAppendEntries()
		case <-r.toggle:
			return
		case <-r.stop:
			return
		}
	}
}

func (r *Raft) cmdToFuture(cmd []byte, kind commonpb.EntryType) (raft.PromiseEntry, raft.Future, error) {
	r.mu.Lock()
	state := r.state
	leader := r.leader
	term := r.currentTerm
	r.mu.Unlock()

	if state != Leader {
		return nil, nil, raft.ErrNotLeader{Leader: leader}
	}

	entry := &commonpb.Entry{
		EntryType: kind,
		Term:      term,
		Data:      cmd,
	}

	promise, future := raft.NewPromiseEntry(entry)

	return promise, future, nil
}

func (r *Raft) advanceCommitIndex() {
	r.mu.Lock()
	defer r.mu.Unlock()

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

		r.newCommit(max(old, r.appliedIndex))
	}

	for _, future := range r.pendingReads {
		r.applyCh <- future
		rmetrics.reads.Add(1)
	}

	r.pendingReads = nil
}

// TODO Assumes caller already holds lock on Raft.
func (r *Raft) newCommit(old uint64) {
	for i := old + 1; i <= r.commitIndex; i++ {
		if i < r.appliedIndex {
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
				future := e.Value.(raft.PromiseLogEntry)
				if future.Entry().Index == i {
					r.applyCh <- future
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
			r.applyCh <- raft.NewPromiseNoFuture(committed)
		}
	}
}

func (r *Raft) runStateMachine() {
	apply := func(promise raft.PromiseLogEntry) {
		var res interface{}
		entry := promise.Entry()

		switch entry.EntryType {
		case commonpb.EntryInternal:
			r.sm.Apply(entry)
		case commonpb.EntryNormal:
			res = r.sm.Apply(entry)
		case commonpb.EntryReconf:
			// TODO We should be able to skip the unmarshaling if we
			// are not recovering.
			var reconf commonpb.ReconfRequest
			err := reconf.Unmarshal(entry.Data)

			if err != nil {
				panic("could not unmarshal reconf")
			}

			r.mem.setPending(&reconf)
			r.mem.set(entry.Index)
			r.logger.Warnln("Comitted configuration")

			enabled := r.mem.commit()

			switch reconf.ReconfType {
			case commonpb.ReconfAdd:
				r.event.Record(raft.EventAdded)
			case commonpb.ReconfRemove:
				r.event.Record(raft.EventRemoved)
			}

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

			// Inform state machine about new configuration.
			r.sm.Apply(entry)
		}

		promise.Respond(res)
		dur := promise.Duration()
		start := time.Now().Add(-dur)
		r.lat.Record(start)
		if r.metricsEnabled {
			rmetrics.cmdCommit.Observe(dur.Seconds())
		}
	}

	for {
		select {
		case commit := <-r.applyCh:
			apply(commit)
		case <-r.stop:
			return
		}
	}
}

// TODO Assumes caller holds lock on Raft.
func (r *Raft) startElection() {
	r.state = Candidate
	term := r.currentTerm + 1

	if !r.preElection {
		r.event.Record(raft.EventElection)
		// We are now a candidate. See Raft Paper Figure 2 -> Rules for Servers -> Candidates.
		// #C1 Increment currentTerm.
		r.currentTerm++
		r.storage.Set(raft.KeyTerm, r.currentTerm)

		// #C2 Vote for self.
		r.votedFor = r.id
		r.storage.Set(raft.KeyVotedFor, r.id)
	} else {
		r.event.Record(raft.EventPreElection)
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
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.inflight > r.maxInflight {
		return
	}

	r.inflight++

	var toSave []*commonpb.Entry
	assignIndex := r.storage.NextIndex()

	// TODO This means the first entry in the log cannot be a configuration
	// change. This should not pose a problem as we always commit a no-op
	// first. This needs to be dealt with if we decide to store the initial
	// configuration at the first index however.
	var reconf uint64

LOOP:
	for i := r.entriesPerMsg; i > 0; i-- {
		select {
		case promise := <-r.queue:
			promiseEntry := promise.Write(assignIndex)
			entry := promiseEntry.Entry()

			if entry.EntryType == commonpb.EntryReconf {
				reconf = assignIndex
			}
			assignIndex++
			toSave = append(toSave, entry)
			r.pending.PushBack(promiseEntry)
		default:
			break LOOP
		}
	}

	if len(toSave) > 0 {
		r.storage.StoreEntries(toSave)
	}

	if reconf > 0 {
		r.mem.set(reconf)
		r.event.Record(raft.EventApplyConfiguration)
	}

	r.aereqout <- r.getAppendEntriesRequest(r.nextIndex, nil)
}

// TODO Assumes caller holds lock on Raft.
func (r *Raft) getNextEntries(nextIndex uint64) []*commonpb.Entry {
	var entries []*commonpb.Entry

	next := nextIndex
	logLen := r.storage.NextIndex() - 1

	if next <= logLen {
		maxEntries := min(next+r.burst, logLen)

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
	if r.state == Leader {
	EMPTYCH:
		for {
			// Empty queue.
			select {
			case promise := <-r.queue:
				promise.Respond(raft.ErrNotLeader{Leader: r.leader})
			default:
				break EMPTYCH
			}
		}
	}

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
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.state
}
