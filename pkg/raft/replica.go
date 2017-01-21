package raft

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	gorums "github.com/relab/raft/pkg/raft/gorumspb"
	pb "github.com/relab/raft/pkg/raft/raftpb"
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
// This must be a value which cannot be returned from idutil.IDFromAddress(address).
const None = 0

// BufferSize is the initial buffer size used for maps and buffered channels
// that directly depend on the number of requests being serviced.
const BufferSize = 10000

var (
	// ErrLateCommit indicates an entry taking too long to commit.
	ErrLateCommit = errors.New("Entry not committed in time.")

	errCommaInCommand = errors.New("Comma in command.")
)

// Config contains the configuration need to start a Replica.
type Config struct {
	ID uint64

	Nodes []string

	Recover          bool
	Batch            bool
	QRPC             bool
	SlowQuorum       bool
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	MaxAppendEntries int

	Logger *log.Logger
}

type uniqueCommand struct {
	clientID       uint32
	sequenceNumber uint64
}

type persistent struct {
	currentTerm uint64
	votedFor    uint64
	log         []*pb.Entry
	commands map[uniqueCommand]*pb.ClientCommandRequest

	recoverFile *os.File
}

// Replica represents a Raft server.
type Replica struct {
	// Must be acquired before mutating Replica state.
	sync.Mutex

	id     uint64
	leader uint64

	persistent *persistent

	seenLeader      bool
	heardFromLeader bool

	state State

	qs   *QuorumSpec
	conf *gorums.Configuration

	addrs []string
	nodes map[uint64]*gorums.Node

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

	pending map[uniqueCommand]chan<- *pb.ClientCommandRequest

	maxAppendEntries int
	queue            chan *pb.Entry

	batch bool
	qrpc  bool

	logger *Logger
}

// NewReplica returns a new Replica given a configuration.
func NewReplica(cfg *Config) (*Replica, error) {
	if cfg.Logger == nil {
		cfg.Logger = log.New(ioutil.Discard, "", 0)
	}

	electionTimeout := randomTimeout(cfg.ElectionTimeout)

	heartbeat := NewTimer(0)
	heartbeat.Stop()

	// Recover from stable storage. TODO Cleanup.
	persistent, err := recoverFromStable(cfg.ID, cfg.Recover)

	if err != nil {
		return nil, err
	}

	var nodes []string

	// Remove self
	for _, node := range cfg.Nodes {
		if node == cfg.Nodes[cfg.ID-1] {
			continue
		}

		nodes = append(nodes, node)
	}

	// TODO Order.
	r := &Replica{
		id:               cfg.ID,
		batch:            cfg.Batch,
		addrs:            nodes,
		nodes:            make(map[uint64]*gorums.Node, len(nodes)),
		qs:               newQuorumSpec(len(nodes)),
		nextIndex:        1,
		baselineTimeout:  cfg.ElectionTimeout,
		electionTimeout:  electionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		// TODO Fix Timer. Create new Timer instead of reusing.
		baseline:         NewTimer(cfg.ElectionTimeout),
		election:         NewTimer(randomTimeout(cfg.ElectionTimeout)),
		heartbeat:        heartbeat,
		preElection:      true,
		pending:          make(map[uniqueCommand]chan<- *pb.ClientCommandRequest, BufferSize),
		maxAppendEntries: cfg.MaxAppendEntries,
		queue:            make(chan *pb.Entry, BufferSize),
		persistent:       persistent,
		logger:           &Logger{cfg.ID, cfg.Logger},
	}

	r.logger.Printf("ElectionTimeout: %v", r.electionTimeout)

	// Makes sure no RPCs are processed until the replica is ready.
	r.Lock()

	return r, nil
}

// Run handles timeouts.
// All RPCs are handled by Gorums.
func (r *Replica) Run() error {
	if err := r.connect(); err != nil {
		return err
	}

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

// RequestVote handles a RequestVoteRequest which is invoked by candidates to gather votes.
// See Raft paper § 5.2.
func (r *Replica) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	r.Lock()
	defer r.Unlock()

	pre := ""

	if request.PreVote {
		pre = "Pre"
	}

	if logLevel >= INFO {
		r.logger.from(request.CandidateID, fmt.Sprintf("%sVote requested in term %d for term %d", pre, r.persistent.currentTerm, request.Term))
	}

	// #RV1 Reply false if term < currentTerm.
	if request.Term < r.persistent.currentTerm {
		return &pb.RequestVoteResponse{Term: r.persistent.currentTerm}, nil
	}

	if request.PreVote && r.heardFromLeader {
		// We don't grant pre-votes if we have recently heard from a
		// leader.
		return &pb.RequestVoteResponse{Term: r.persistent.currentTerm}, nil
	}

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if request.Term > r.persistent.currentTerm && !request.PreVote {
		r.becomeFollower(request.Term)
	}

	// #RV2 If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
	if (r.persistent.votedFor == None || r.persistent.votedFor == request.CandidateID) &&
		(request.LastLogTerm > r.logTerm(len(r.persistent.log)) ||
			(request.LastLogTerm == r.logTerm(len(r.persistent.log)) && request.LastLogIndex >= uint64(len(r.persistent.log)))) {
		if logLevel >= INFO {
			r.logger.to(request.CandidateID, fmt.Sprintf("%sVote granted for term %d", pre, request.Term))
		}

		if request.PreVote {
			return &pb.RequestVoteResponse{VoteGranted: true, Term: request.Term}, nil
		}

		r.persistent.votedFor = request.CandidateID

		// Write to stable storage
		// TODO Assumes successful
		r.save(fmt.Sprintf(STOREVOTEDFOR, r.persistent.votedFor))

		// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader or granting a vote to candidate: convert to candidate.
		// Here we are granting a vote to a candidate so we reset the election timeout.
		r.election.Reset(r.electionTimeout)
		r.baseline.Reset(r.baselineTimeout)

		return &pb.RequestVoteResponse{VoteGranted: true, Term: r.persistent.currentTerm}, nil
	}

	// #RV2 The candidate's log was not up-to-date
	return &pb.RequestVoteResponse{Term: r.persistent.currentTerm}, nil
}

// AppendEntries invoked by leader to replicate log entries, also used as a heartbeat.
// See Raft paper § 5.3 and § 5.2.
func (r *Replica) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	r.Lock()
	defer r.Unlock()

	if logLevel >= TRACE {
		r.logger.from(request.LeaderID, fmt.Sprintf("AppendEntries = %+v", request))
	}

	// #AE1 Reply false if term < currentTerm.
	if request.Term < r.persistent.currentTerm {
		return &pb.AppendEntriesResponse{
			Success: false,
			Term:    request.Term,
		}, nil
	}

	success := request.PrevLogIndex == 0 || (request.PrevLogIndex-1 < uint64(len(r.persistent.log)) && r.persistent.log[request.PrevLogIndex-1].Term == request.PrevLogTerm)

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if request.Term > r.persistent.currentTerm {
		r.becomeFollower(request.Term)
	} else if r.id != request.LeaderID {
		r.becomeFollower(r.persistent.currentTerm)
	}

	if success {
		r.leader = request.LeaderID
		r.heardFromLeader = true
		r.seenLeader = true

		index := int(request.PrevLogIndex)

		var buffer bytes.Buffer

		for _, entry := range request.Entries {
			index++

			if index == len(r.persistent.log) || r.logTerm(index) != entry.Term {
				r.persistent.log = r.persistent.log[:index-1] // Remove excessive log entries.
				r.persistent.log = append(r.persistent.log, entry)

				buffer.WriteString(fmt.Sprintf(STORECOMMAND, entry.Term, entry.Data.ClientID, entry.Data.SequenceNumber, entry.Data.Command))
			}
		}

		// Write to stable storage
		// TODO Assumes successful
		r.save(buffer.String())

		if logLevel >= DEBUG {
			r.logger.to(request.LeaderID, fmt.Sprintf("AppendEntries persisted %d entries to stable storage", len(request.Entries)))
		}

		old := r.commitIndex

		r.commitIndex = min(request.CommitIndex, uint64(index))

		if r.commitIndex > old {
			r.newCommit(old)
		}
	}

	return &pb.AppendEntriesResponse{
		Term:       request.Term,
		MatchIndex: uint64(len(r.persistent.log)),
		Success:    success,
	}, nil
}

// ClientCommand is invoked by a client to commit a command.
// See Raft paper § 8 and the Raft PhD dissertation chapter 6.
func (r *Replica) ClientCommand(ctx context.Context, request *pb.ClientCommandRequest) (*pb.ClientCommandResponse, error) {
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

func (r *Replica) connect() error {
	opts := []gorums.ManagerOption{
		gorums.WithGrpcDialOptions(
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(TCPConnect*time.Millisecond)),
	}

	// TODO In main? We really only want the conf and nodes.
	mgr, err := gorums.NewManager(r.addrs, opts...)

	if err != nil {
		return err
	}

	r.conf, err = mgr.NewConfiguration(mgr.NodeIDs(), r.qs)

	if err != nil {
		return err
	}

	var rid uint64 = 1

	for i, node := range mgr.Nodes() {
		// Increase id to compensate gap.
		if r.id == uint64(i+1) {
			rid++
		}

		r.nodes[rid] = node

		rid++
	}

	return nil
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

		commandID := uniqueCommand{clientID: request.ClientID, sequenceNumber: request.SequenceNumber}

		if old, ok := r.persistent.commands[commandID]; ok {
			response := make(chan *pb.ClientCommandRequest, 1)
			response <- old

			return response, true
		}

		r.Unlock()
		r.queue <- &pb.Entry{Term: r.persistent.currentTerm, Data: request}
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

	if r.state == Leader && r.logTerm(r.matchIndex) == r.persistent.currentTerm {
		r.commitIndex = max(r.commitIndex, uint64(r.matchIndex))
	}

	if r.commitIndex > old {
		r.newCommit(old)
	}
}

// TODO Assumes caller already holds lock on Replica
func (r *Replica) newCommit(old uint64) {
	for i := old; i < r.commitIndex; i++ {
		committed := r.persistent.log[i]
		sequenceNumber := committed.Data.SequenceNumber
		clientID := committed.Data.ClientID
		commandID := uniqueCommand{clientID: clientID, sequenceNumber: sequenceNumber}

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

		r.persistent.commands[commandID] = committed.Data
	}
}

func (r *Replica) startElection() {
	r.Lock()
	defer r.Unlock()

	r.state = Candidate
	r.electionTimeout = randomTimeout(r.electionTimeout)

	term := r.persistent.currentTerm + 1
	pre := "pre"

	if !r.preElection {
		pre = ""
		// We are now a candidate. See Raft Paper Figure 2 -> Rules for Servers -> Candidates.
		// #C1 Increment currentTerm.
		r.persistent.currentTerm++

		var buffer bytes.Buffer
		buffer.WriteString(fmt.Sprintf(STORETERM, r.persistent.currentTerm))

		// #C2 Vote for self.
		r.persistent.votedFor = r.id

		buffer.WriteString(fmt.Sprintf(STOREVOTEDFOR, r.persistent.votedFor))

		// Write to stable storage
		// TODO Assumes successful
		r.save(buffer.String())
	}

	if logLevel >= INFO {
		r.logger.log(fmt.Sprintf("Starting %s election for term %d", pre, term))
	}

	// #C3 Reset election timer.
	r.election.Reset(r.electionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)

	// #C4 Send RequestVote RPCs to all other servers.
	req := r.conf.RequestVoteFuture(ctx, &pb.RequestVoteRequest{
		CandidateID:  r.id,
		Term:         term,
		LastLogTerm:  r.logTerm(len(r.persistent.log)),
		LastLogIndex: uint64(len(r.persistent.log)),
		PreVote:      r.preElection,
	})

	go func() {
		reply, err := req.Get()
		cancel()

		if err != nil {
			r.logger.log(fmt.Sprintf("RequestVote failed = %v", err))

			return
		}

		r.handleRequestVoteResponse(reply.RequestVoteResponse)
	}()

	// Election is now started. Election will be continued in handleRequestVote when a response from Gorums is received.
	// See RequestVoteQF for the quorum function creating the response.
}

func (r *Replica) handleRequestVoteResponse(response *pb.RequestVoteResponse) {
	r.Lock()
	defer r.Unlock()

	term := r.persistent.currentTerm

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
			r.logger.log(fmt.Sprintf("Elected leader for term %d", r.persistent.currentTerm))
		}

		r.state = Leader
		r.leader = r.id
		r.seenLeader = true
		r.nextIndex = len(r.persistent.log) + 1

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
		r.logger.log(fmt.Sprintf("Sending AppendEntries for term %d", r.persistent.currentTerm))
	}

	var buffer bytes.Buffer

LOOP:
	for i := r.maxAppendEntries; i > 0; i-- {
		select {
		case entry := <-r.queue:
			r.persistent.log = append(r.persistent.log, entry)

			buffer.WriteString(fmt.Sprintf(STORECOMMAND, r.persistent.currentTerm, entry.Data.ClientID, entry.Data.SequenceNumber, entry.Data.Command))
		default:
			break LOOP
		}
	}

	// Write to stable storage
	// TODO Assumes successful
	r.save(buffer.String())

	// #L1
	entries := []*pb.Entry{}

	next := r.nextIndex - 1

	if len(r.persistent.log) > next {
		maxEntries := int(min(uint64(next+r.maxAppendEntries), uint64(len(r.persistent.log))))

		if !r.batch {
			maxEntries = next + 1
		}

		entries = r.persistent.log[next:maxEntries]
	}

	if logLevel >= DEBUG {
		r.logger.log(fmt.Sprintf("Sending %d entries", len(entries)))
	}

	req := &pb.AppendEntriesRequest{
		LeaderID:     r.id,
		Term:         r.persistent.currentTerm,
		PrevLogIndex: uint64(next),
		PrevLogTerm:  r.logTerm(next),
		CommitIndex:  r.commitIndex,
		Entries:      entries,
	}

	go func(req *pb.AppendEntriesRequest) {
		ctx, cancel := context.WithTimeout(context.Background(), TCPHeartbeat*time.Millisecond)

		resp, err := r.conf.AppendEntries(ctx, req)

		if err != nil {
			r.logger.log(fmt.Sprintf("AppendEntries failed = %v", err))

			if resp.AppendEntriesResponse == nil {
				return
			}
		}

		if !resp.AppendEntriesResponse.Success {
			cancel()
		}

		r.handleAppendEntriesResponse(resp.AppendEntriesResponse)
	}(req)

	r.heartbeat.Reset(r.heartbeatTimeout)
}

func (r *Replica) handleAppendEntriesResponse(response *pb.AppendEntriesResponse) {
	r.Lock()
	defer func() {
		r.Unlock()
		r.advanceCommitIndex()
	}()

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if response.Term > r.persistent.currentTerm {
		r.becomeFollower(response.Term)

		return
	}

	// Ignore late response
	if response.Term < r.persistent.currentTerm {
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

func (r *Replica) becomeFollower(term uint64) {
	r.state = Follower
	r.preElection = true

	if r.persistent.currentTerm != term {
		if logLevel >= INFO {
			r.logger.log(fmt.Sprintf("Become follower as we transition from term %d to %d", r.persistent.currentTerm, term))
		}

		r.persistent.currentTerm = term

		var buffer bytes.Buffer
		buffer.WriteString(fmt.Sprintf(STORETERM, r.persistent.currentTerm))

		if r.persistent.votedFor != None {
			r.persistent.votedFor = None

			buffer.WriteString(fmt.Sprintf(STOREVOTEDFOR, r.persistent.votedFor))
		}

		// Write to stable storage
		// TODO Assumes successful
		r.save(buffer.String())
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
	if index < 1 || index > len(r.persistent.log) {
		return 0
	}

	return r.persistent.log[index-1].Term
}

func (r *Replica) save(buffer string) {
	r.persistent.recoverFile.WriteString(buffer)
	r.persistent.recoverFile.Sync()
}
