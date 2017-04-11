package raftgorums

import (
	"container/list"
	"sync"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
	gorums "github.com/relab/raft/raftgorums/gorumspb"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

// LogLevel sets the level of logging.
const LogLevel = logrus.InfoLevel

// State represents one of the Raft server states.
type State int

// Server states.
const (
	Inactive State = iota
	Follower
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

	storage *PanicStorage

	seenLeader      bool
	heardFromLeader bool

	state State

	addrs []string

	lookup  map[uint64]int
	peers   []string
	cluster []uint64

	match map[uint32]chan uint64

	mgr  *gorums.Manager
	conf *gorums.Configuration

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

	confChange chan *raft.ConfChangeFuture

	logger logrus.FieldLogger

	metricsEnabled bool
}

type catchUpReq struct {
	leaderID   uint64
	matchIndex uint64
}

type entryFuture struct {
	entry  *commonpb.Entry
	future *raft.EntryFuture
}

// NewRaft returns a new Raft given a configuration.
func NewRaft(sm raft.StateMachine, cfg *Config) *Raft {
	// TODO Validate config, i.e., make sure to sensible defaults if an
	// option is not configured.
	storage := &PanicStorage{NewCacheStorage(cfg.Storage, 20000), cfg.Logger}

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
		confChange:       make(chan *raft.ConfChangeFuture),
		logger:           cfg.Logger,
		metricsEnabled:   cfg.MetricsEnabled,
	}

	return r
}

// Run starts a server running the Raft algorithm.
func (r *Raft) Run(server *grpc.Server) error {
	r.initPeers()

	opts := []gorums.ManagerOption{
		gorums.WithGrpcDialOptions(
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(TCPConnect*time.Millisecond)),
	}

	mgr, err := gorums.NewManager(r.peers, opts...)

	if err != nil {
		return err
	}

	gorums.RegisterRaftServer(server, r)

	r.mgr = mgr

	var clusterIDs []uint32

	for _, id := range r.cluster {
		if r.id == id {
			// Exclude self.
			r.state = Follower
			continue
		}
		r.logger.WithField("serverid", id).Warnln("Added to cluster")
		clusterIDs = append(clusterIDs, r.getNodeID(id))
	}

	r.conf, err = mgr.NewConfiguration(clusterIDs, NewQuorumSpec(len(clusterIDs)+1))

	if err != nil {
		return err
	}

	for _, nodeID := range r.mgr.NodeIDs() {
		r.match[nodeID] = make(chan uint64, 1)
	}

	go r.run()

	return r.handleOutgoing()
}

func (r *Raft) handleOutgoing() error {
	// January 1, 1970 UTC.
	var lastCuReq time.Time

	for {
		select {
		case err := <-r.conf.SubError():
			// TODO If a node becomes unavailable and there is a
			// backup available in the same or an alternate region,
			// instantiate reconfiguratior. TODO How many errors
			// before a node is considered unavailable? If there is
			// no backup node available, don't do anything, but
			// schedule the reconfiguratior.
			r.logger.WithField("nodeid", err.NodeID).Warnln("Node unavailable")
		case req := <-r.cureqout:
			// TODO Use config.
			if time.Since(lastCuReq) < 100*time.Millisecond {
				continue
			}
			lastCuReq = time.Now()

			r.logger.WithField("matchindex", req.matchIndex).Warnln("Sending catch-up")
			ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
			leader, _ := r.mgr.Node(r.getNodeID(req.leaderID))
			_, err := leader.RaftClient.CatchMeUp(ctx, &pb.CatchMeUpRequest{
				FollowerID: r.id,
				NextIndex:  req.matchIndex + 1,
			})
			cancel()

			if err != nil {
				r.logger.WithError(err).Warnln("CatchMeUp failed")
			}
		case req := <-r.rvreqout:
			ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
			res, err := r.conf.RequestVote(ctx, req)
			cancel()

			if err != nil {
				r.logger.WithError(err).Warnln("RequestVote failed")
			}

			if res == nil {
				continue
			}

			r.HandleRequestVoteResponse(res)

		case req := <-r.aereqout:
			next := make(map[uint32]uint64)
			nextIndex := req.PrevLogIndex + 1

			for nodeID, ch := range r.match {
				select {
				case index := <-ch:
					// TODO Acessing maxAppendEntries, safe but needs fix.
					atLeastMaxEntries := req.PrevLogIndex+1 > r.maxAppendEntries
					lessThenMaxEntriesBehind := index < req.PrevLogIndex+1-r.maxAppendEntries

					if atLeastMaxEntries && lessThenMaxEntriesBehind {
						r.logger.WithField("gorumsid", nodeID).Warnln("Server too far behind")
						index = req.PrevLogIndex + 1
					}
					next[nodeID] = index
					if index < nextIndex {
						nextIndex = index
					}
				default:
				}
			}

			// TODO This should be safe as it only accesses storage
			// which uses transactions. TODO It accesses
			// maxAppendEntries but this on does not change after
			// startup.
			entries := r.getNextEntries(nextIndex)
			e := uint64(len(entries))
			maxIndex := nextIndex + e - 1

			ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)
			res, err := r.conf.AppendEntries(ctx, req,
				// These functions will be executed concurrently.
				func(req pb.AppendEntriesRequest, nodeID uint32) *pb.AppendEntriesRequest {
					if index, ok := next[nodeID]; ok {
						req.PrevLogIndex = index - 1
						// TODO This should be safe as
						// it only accesses storage
						// which uses transactions.
						req.PrevLogTerm = r.logTerm(index - 1)
					}

					need := maxIndex - req.PrevLogIndex
					req.Entries = entries[e-need:]

					r.logger.WithFields(logrus.Fields{
						"prevlogindex": req.PrevLogIndex,
						"prevlogterm":  req.PrevLogTerm,
						"commitindex":  req.CommitIndex,
						"currentterm":  req.Term,
						"lenentries":   len(req.Entries),
						"gorumsid":     nodeID,
					}).Infoln("Sending AppendEntries")

					return &req
				},
			)

			if err != nil {
				r.logger.WithError(err).Warnln("AppendEntries failed")
			}

			if res == nil {
				continue
			}

			// Cancel on abort.
			if !res.Success {
				cancel()
			}

			r.HandleAppendEntriesResponse(res, res.Replies)
		}
	}
}

func (r *Raft) initPeers() {
	peers := make([]string, len(r.addrs))
	// We don't want to mutate r.addrs.
	copy(peers, r.addrs)

	// Exclude self.
	r.peers = append(peers[:r.id-1], peers[r.id:]...)

	var pos int
	r.lookup = make(map[uint64]int)

	for i := 1; i <= len(r.addrs); i++ {
		if uint64(i) == r.id {
			continue
		}

		r.lookup[uint64(i)] = pos
		pos++
	}
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

	lastIndex := r.storage.NextIndex() - 1
	lastLogTerm := r.logTerm(lastIndex)

	// We can grant a vote in the same term, as long as it's to the same
	// candidate. This is useful if the response was lost, and the candidate
	// sends another request.
	alreadyVotedForCandidate := r.votedFor == req.CandidateID

	// If the logs have last entries with different terms, the log with the
	// later term is more up-to-date.
	laterTerm := req.LastLogTerm > lastLogTerm

	// If the logs end with the same term, whichever log is longer is more
	// up-to-date.
	longEnough := req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastIndex

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
		"prevlogterm":  req.PrevLogTerm,
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

	logLen := r.storage.NextIndex() - 1
	prevTerm := r.logTerm(req.PrevLogIndex)

	// An AppendEntries request is always successful for the first index. A
	// leader can only be elected leader if its log matches that of a
	// majority and our log is guaranteed to be at least 0 in length.
	firstIndex := req.PrevLogIndex == 0

	// The index preceding the entries we are going to replicate must be in our log.
	gotPrevIndex := req.PrevLogIndex <= logLen
	// The term must match to satisfy the log matching property.
	sameTerm := req.PrevLogTerm == prevTerm

	// If the previous entry is in our log, then our log matches the leaders
	// up till and including the previous entry. And we can safely replicate
	// next new entries.
	gotPrevEntry := gotPrevIndex && sameTerm

	success := firstIndex || gotPrevEntry

	// #A2 If RPC request or response contains term T > currentTerm: set
	// currentTerm = T, convert to follower.
	if req.Term > r.currentTerm {
		r.becomeFollower(req.Term)
	} else if r.id != req.LeaderID {
		r.becomeFollower(r.currentTerm)
	}

	if r.metricsEnabled {
		rmetrics.leader.Set(float64(req.LeaderID))
	}

	// We acknowledge this server as the leader as it's has the highest term
	// we have seen, and there can only be one leader per term.
	r.leader = req.LeaderID
	r.heardFromLeader = true
	r.seenLeader = true

	if !success {
		r.cureqout <- &catchUpReq{
			leaderID:   req.LeaderID,
			matchIndex: r.storage.NextIndex() - 1,
		}

		return &pb.AppendEntriesResponse{
			Term:       req.Term,
			MatchIndex: r.storage.NextIndex() - 1,
		}
	}

	// If we already know that the entries we are receiving are committed in
	// our log, we can return early.
	if req.CommitIndex < r.commitIndex {
		return &pb.AppendEntriesResponse{
			Term:       req.Term,
			MatchIndex: r.storage.NextIndex() - 1,
			Success:    success,
		}
	}

	var toSave []*commonpb.Entry
	index := req.PrevLogIndex

	for _, entry := range req.Entries {
		// Increment first so we start at previous index + 1.
		index++

		// If the terms don't match, our logs conflict at this index. On
		// the first conflict this will truncate the log to the lowest
		// common matching index. After that it will fill the log with
		// the new entries from the leader. This is because entry.Term
		// will always conflict with term 0, which will be returned for
		// indexes outside our log.
		if entry.Term != r.logTerm(index) {
			logLen = r.storage.NextIndex() - 1
			for logLen > index-1 {
				r.storage.RemoveEntries(logLen, logLen)
				logLen = r.storage.NextIndex() - 1
			}
			toSave = append(toSave, entry)
		}
	}

	if len(toSave) > 0 {
		r.storage.StoreEntries(toSave)
	}
	logLen = r.storage.NextIndex() - 1

	old := r.commitIndex
	// Commit index can not exceed the length of our log.
	r.commitIndex = min(req.CommitIndex, logLen)

	if r.metricsEnabled {
		rmetrics.commitIndex.Set(float64(r.commitIndex))
	}

	if r.commitIndex > old {
		r.logger.WithFields(logrus.Fields{
			"oldcommitindex": old,
			"commitindex":    r.commitIndex,
		}).Infoln("Set commit index")

		r.newCommit(old)
	}

	reqLogger.WithFields(logrus.Fields{
		"lensaved":   len(toSave),
		"lenlog":     logLen,
		"matchindex": index,
		"success":    success,
	}).Infoln("Saved entries to stable storage")

	return &pb.AppendEntriesResponse{
		Term:       req.Term,
		MatchIndex: index,
		Success:    success,
	}
}

// ProposeConf implements raft.Raft.
func (r *Raft) ProposeConf(ctx context.Context, confChange *commonpb.ConfChangeRequest) (raft.Future, error) {
	cmd, err := confChange.Marshal()

	if err != nil {
		return nil, err
	}

	future, err := r.cmdToFuture(cmd, commonpb.EntryConfChange)

	if err != nil {
		return nil, err
	}

	confFuture := &raft.ConfChangeFuture{
		Req:         confChange,
		EntryFuture: future,
	}

	select {
	case r.confChange <- confFuture:
		return confFuture, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ProposeCmd implements raft.Raft.
func (r *Raft) ProposeCmd(ctx context.Context, cmd []byte) (raft.Future, error) {
	future, err := r.cmdToFuture(cmd, commonpb.EntryNormal)

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
	future, err := r.cmdToFuture(cmd, commonpb.EntryNormal)

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
		r.storage.Set(KeyTerm, r.currentTerm)

		// #C2 Vote for self.
		r.votedFor = r.id
		r.storage.Set(KeyVotedFor, r.id)
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

		logLen := r.storage.NextIndex() - 1

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
	assignIndex := r.storage.NextIndex()

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

	if len(toSave) > 0 {
		r.storage.StoreEntries(toSave)
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

// HandleAppendEntriesResponse must be invoked when receiving an
// AppendEntriesResponse.
func (r *Raft) HandleAppendEntriesResponse(response *pb.AppendEntriesQFResponse, replies uint64) {
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
	if response.Term > r.currentTerm || replies < uint64(len(r.addrs)/2) {
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

func (r *Raft) getNodeID(raftID uint64) uint32 {
	return r.mgr.NodeIDs()[r.lookup[raftID]]
}
