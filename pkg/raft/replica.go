package raft

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/relab/raft/pkg/raft/raftpb"
)

// LogLevel sets the level of log verbosity.
type LogLevel int

// Log verbosity levels.
const (
	OFF LogLevel = iota
	INFO
	DEBUG
	TRACE
)

const logLevel = DEBUG

// State represents one of the Raft server states.
type State int

// Server states.
const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

// Timeouts in milliseconds.
const (
	// How long we wait for an answer.
	TCPCONNECT   = 5000
	TCPHEARTBEAT = 2000

	// Raft RPC timeouts.
	HEARTBEAT = 250
	ELECTION  = 2000
)

// NONE represents no server.
// This must be a value which cannot be returned from idutil.IDFromAddress(address).
const NONE = 0

// MAXENTRIES is the maximum number of entries a AppendEntries RPC can carry at once.
// It should be greater than the expected throughput of the system.
// It is used to stabilize the system, giving greater throughput at the potential cost of latency.
const MAXENTRIES = 10000 / (1000 / HEARTBEAT)

// BUFFERSIZE is the initial buffer size used for maps and buffered channels
// that directly depend on the number of requests being serviced.
const BUFFERSIZE = 10000

// How events are persisted to stable storage.
const (
	// TODO Should be in config. Default to tmp.
	STOREFILE     = "%d.storage"
	STORETERM     = "TERM,%d\n"
	STOREVOTEDFOR = "VOTED,%d\n"
	STORECOMMAND  = "%d,%d,%d,%s\n"
)

var (
	// ErrLateCommit indicates an entry taking too long to commit.
	ErrLateCommit = errors.New("Entry not committed in time.")

	errCommaInCommand = errors.New("Comma in command.")
)

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}

	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}

	return b
}

func lookupTables(raftOrder []string, nodeOrder []uint32) (map[uint32]uint64, map[uint64]uint32, error) {
	h := fnv.New32a()

	raftID := make(map[uint32]uint64)
	nodeID := make(map[uint64]uint32)

	for _, id := range nodeOrder {
		raftID[id] = 0
	}

	for rid, addr := range raftOrder {
		tcpAddr, err := net.ResolveTCPAddr("tcp", addr)

		if err != nil {
			return nil, nil, err
		}

		_, _ = h.Write([]byte(tcpAddr.String()))
		nid := h.Sum32()
		h.Reset()

		raftID[nid] = uint64(rid + 1)
		nodeID[uint64(rid+1)] = nid
	}

	return raftID, nodeID, nil
}

func randomTimeout() time.Duration {
	return time.Duration(ELECTION+rand.Intn(ELECTION*2-ELECTION)) * time.Millisecond
}

// Config contains the configuration need to start a Replica.
type Config struct {
	ID uint64

	Nodes []string

	Recover    bool
	Batch      bool
	QRPC       bool
	SlowQuorum bool
}

func (r *Replica) save(buffer string) {
	r.persistent.recoverFile.WriteString(buffer)
	r.persistent.recoverFile.Sync()
}

type appendEntriesHandler interface {
	sendRequest(*Replica)
	handleResponse(*Replica, *pb.AppendEntriesResponse)
}

// Replica represents a Raft server.
type Replica struct {
	// Must be acquired before mutating Replica state.
	sync.Mutex

	id     uint64
	leader uint64

	persistent *persistent

	seenLeader bool

	state State

	qs   *QuorumSpec
	conf *pb.Configuration

	addrs []string
	nodes map[uint64]*pb.Node

	raftID map[uint32]uint64
	nodeID map[uint64]uint32

	commitIndex uint64

	nextIndex  map[uint64]int
	matchIndex map[uint64]int

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	election  Timer
	heartbeat Timer

	pending map[uniqueCommand]chan<- *pb.ClientCommandRequest

	queue chan *pb.Entry

	batch bool
	qrpc  bool

	aeHandler appendEntriesHandler

	// TODO Fix logging.
	logLocal func(string)
	logFrom  func(uint64, string)
	logTo    func(uint64, string)
}

type uniqueCommand struct {
	clientID       uint32
	sequenceNumber uint64
}

func (r *Replica) logTerm(index int) uint64 {
	if index < 1 || index > len(r.persistent.log) {
		return 0
	}

	return r.persistent.log[index-1].Term
}

func newQuorumSpec(peers int, slow bool) *QuorumSpec {
	n := peers + 1
	sq := n / 2

	if slow {
		sq = n - 1
	}

	return &QuorumSpec{
		N:  n,
		SQ: sq,
		FQ: n / 2,
	}
}

// NewReplica returns a new Replica given a configuration.
func NewReplica(cfg *Config) (*Replica, error) {
	var aeHandler appendEntriesHandler

	if cfg.QRPC {
		aeHandler = &aeqrpc{}
	} else {
		aeHandler = &aenoqrpc{}
	}

	// TODO Timeouts should be in config.
	electionTimeout := randomTimeout()

	heartbeat := NewTimer(0)
	heartbeat.Stop()

	// Recover from stable storage. TODO Cleanup.
	persistent, err := recoverFromStable(cfg.ID, cfg.Recover)

	if err != nil {
		return nil, err
	}

	nextIndex := make(map[uint64]int, len(cfg.Nodes))
	matchIndex := make(map[uint64]int, len(cfg.Nodes))

	for i := range cfg.Nodes {
		id := uint64(i + 1)

		// Exclude self.
		if id == cfg.ID {
			continue
		}

		// Initialized per Raft specification.
		nextIndex[id] = 1
		matchIndex[id] = 0
	}

	// TODO Order, i.e., lookup tables before nodes.
	r := &Replica{
		id:               cfg.ID,
		batch:            cfg.Batch,
		qrpc:             cfg.QRPC,
		addrs:            cfg.Nodes,
		nodes:            make(map[uint64]*pb.Node, len(cfg.Nodes)),
		qs:               newQuorumSpec(len(cfg.Nodes), cfg.SlowQuorum),
		nextIndex:        nextIndex,
		matchIndex:       matchIndex,
		electionTimeout:  electionTimeout,
		heartbeatTimeout: HEARTBEAT * time.Millisecond,
		// TODO Fix Timer. Create new Timer instead of reusing.
		election:   NewTimer(electionTimeout),
		heartbeat:  heartbeat,
		pending:    make(map[uniqueCommand]chan<- *pb.ClientCommandRequest, BUFFERSIZE),
		queue:      make(chan *pb.Entry, BUFFERSIZE),
		persistent: persistent,
		aeHandler:  aeHandler,
		// TODO Fix logging.
		logLocal: func(message string) {
			log.Printf("%d: %s", cfg.ID, message)
		},
		logFrom: func(from uint64, message string) {
			log.Printf("%d <- %d: %s", cfg.ID, from, message)
		},
		logTo: func(to uint64, message string) {
			log.Printf("%d -> %d: %s", cfg.ID, to, message)
		},
	}

	// Makes sure no RPCs are processed until the replica is ready.
	r.Lock()

	return r, nil
}

func (r *Replica) connect() error {
	opts := []pb.ManagerOption{
		pb.WithGrpcDialOptions(
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(TCPCONNECT*time.Millisecond),
			grpc.WithBackoffMaxDelay(time.Second)),
		pb.WithSelfAddr(r.addrs[r.id-1]),
	}

	// TODO In main? We really only want the conf and nodes.
	mgr, err := pb.NewManager(r.addrs, opts...)

	if err != nil {
		return err
	}

	// mgr.NodeIDs excluding self.
	var peerIDs []uint32

	for _, node := range mgr.Nodes(true) {
		peerIDs = append(peerIDs, node.ID())
	}

	r.conf, err = mgr.NewConfiguration(peerIDs, r.qs)

	if err != nil {
		return err
	}

	r.raftID, r.nodeID, err = lookupTables(r.addrs, peerIDs)

	if err != nil {
		return err
	}

	for _, node := range mgr.Nodes(true) {
		r.nodes[r.raftID[node.ID()]] = node
	}

	return nil
}

type persistent struct {
	currentTerm uint64
	votedFor    uint64
	log         []*pb.Entry
	commands    map[uniqueCommand]*pb.ClientCommandRequest

	recoverFile *os.File
}

func recoverFromStable(id uint64, recover bool) (*persistent, error) {
	p := &persistent{
		commands: make(map[uniqueCommand]*pb.ClientCommandRequest, BUFFERSIZE),
	}

	recoverFile := fmt.Sprintf(STOREFILE, id)

	if _, err := os.Stat(recoverFile); !os.IsNotExist(err) && recover {
		file, err := os.Open(recoverFile)

		if err != nil {
			return nil, err
		}

		defer file.Close()

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			text := scanner.Text()
			split := strings.Split(text, ",")

			switch split[0] {
			case "TERM":
				term, err := strconv.Atoi(split[1])

				if err != nil {
					return nil, err
				}

				p.currentTerm = uint64(term)
			case "VOTED":
				votedFor, err := strconv.Atoi(split[1])

				if err != nil {
					return nil, err
				}

				p.votedFor = uint64(votedFor)
			default:
				var entry pb.Entry

				if len(split) != 4 {
					return nil, errCommaInCommand
				}

				term, err := strconv.Atoi(split[0])

				if err != nil {
					return nil, err
				}

				clientID, err := strconv.Atoi(split[1])

				if err != nil {
					return nil, err
				}

				sequenceNumber, err := strconv.Atoi(split[2])

				if err != nil {
					return nil, err
				}

				command := split[3]

				entry.Term = uint64(term)
				entry.Data = &pb.ClientCommandRequest{ClientID: uint32(clientID), SequenceNumber: uint64(sequenceNumber), Command: command}

				p.log = append(p.log, &entry)
				p.commands[uniqueCommand{entry.Data.ClientID, entry.Data.SequenceNumber}] = entry.Data
			}
		}

		if err := scanner.Err(); err != nil {
			return nil, err
		}

		// TODO Close if we were to implement graceful shutdown.
		p.recoverFile, err = os.OpenFile(recoverFile, os.O_APPEND|os.O_WRONLY, 0666)

		return p, err
	}

	// TODO Close if we were to implement graceful shutdown.
	var err error
	p.recoverFile, err = os.Create(recoverFile)

	return p, err
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
		case <-r.election.C:
			// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader
			// or granting vote to candidate: convert to candidate.
			r.startElection()

		case <-r.heartbeat.C:
			r.aeHandler.sendRequest(r)
		}
	}
}

// RequestVote handles a RequestVoteRequest which is invoked by candidates to gather votes.
// See Raft paper § 5.2.
func (r *Replica) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	r.Lock()
	defer r.Unlock()

	if logLevel >= INFO {
		r.logFrom(request.CandidateID, fmt.Sprintf("Vote requested in term %d for term %d", r.persistent.currentTerm, request.Term))
	}

	// #RV1 Reply false if term < currentTerm.
	if request.Term < r.persistent.currentTerm {
		return &pb.RequestVoteResponse{Term: r.persistent.currentTerm}, nil
	}

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if request.Term > r.persistent.currentTerm {
		r.becomeFollower(request.Term)
	}

	// #RV2 If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
	if (r.persistent.votedFor == NONE || r.persistent.votedFor == request.CandidateID) &&
		(request.LastLogTerm > r.logTerm(len(r.persistent.log)) ||
			(request.LastLogTerm == r.logTerm(len(r.persistent.log)) && request.LastLogIndex >= uint64(len(r.persistent.log)))) {
		if logLevel >= INFO {
			r.logTo(request.CandidateID, fmt.Sprintf("Vote granted for term %d", request.Term))
		}

		r.persistent.votedFor = request.CandidateID

		// Write to stable storage
		// TODO Assumes successful
		r.save(fmt.Sprintf(STOREVOTEDFOR, r.persistent.votedFor))

		// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader or granting a vote to candidate: convert to candidate.
		// Here we are granting a vote to a candidate so we reset the election timeout.
		r.election.Reset(r.electionTimeout)

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
		r.logFrom(request.LeaderID, fmt.Sprintf("AppendEntries = %+v", request))
	}

	// #AE1 Reply false if term < currentTerm.
	if request.Term < r.persistent.currentTerm {
		return &pb.AppendEntriesResponse{
			FollowerID: []uint64{r.id},
			Success:    false,
			Term:       request.Term,
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
			r.logTo(request.LeaderID, fmt.Sprintf("AppendEntries persisted %d entries to stable storage", len(request.Entries)))
		}

		old := r.commitIndex

		r.commitIndex = min(request.CommitIndex, uint64(index))

		if r.commitIndex > old {
			r.newCommit(old)
		}
	}

	return &pb.AppendEntriesResponse{
		FollowerID: []uint64{r.id},
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
			r.aeHandler.sendRequest(r)
		}

		select {
		// Wait on committed entry.
		case entry := <-response:
			return &pb.ClientCommandResponse{Status: pb.OK, Response: entry.Command, ClientID: entry.ClientID}, nil

		// Return if responding takes too much time.
		// The client will retry.
		case <-time.After(TCPHEARTBEAT * time.Millisecond):
			return nil, ErrLateCommit
		}
	}

	hint := r.getHint()

	return &pb.ClientCommandResponse{Status: pb.NOT_LEADER, LeaderHint: hint}, nil
}

func (r *Replica) logCommand(request *pb.ClientCommandRequest) (<-chan *pb.ClientCommandRequest, bool) {
	r.Lock()
	defer r.Unlock()

	if r.state == LEADER {
		if request.SequenceNumber == 0 {
			request.ClientID = rand.Uint32()

			if logLevel >= INFO {
				r.logFrom(uint64(request.ClientID), fmt.Sprintf("Client request = %s", request.Command))
			}
		} else if logLevel >= TRACE {
			r.logFrom(uint64(request.ClientID), fmt.Sprintf("Client request = %s", request.Command))
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

func (r *Replica) advanceCommitIndex() {
	r.Lock()
	defer r.Unlock()

	matchIndexes := []int{len(r.persistent.log)}

	for _, i := range r.matchIndex {
		matchIndexes = append(matchIndexes, i)
	}

	sort.Ints(matchIndexes)

	atleast := matchIndexes[(len(r.matchIndex)+1)/2]

	old := r.commitIndex

	if r.state == LEADER && r.logTerm(atleast) == r.persistent.currentTerm {
		r.commitIndex = max(r.commitIndex, uint64(atleast))
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
				case <-time.After(TCPHEARTBEAT * time.Millisecond):
				}
			}(response)
		}

		r.persistent.commands[commandID] = committed.Data
	}
}

func (r *Replica) startElection() {
	r.Lock()
	defer r.Unlock()

	r.state = CANDIDATE
	r.electionTimeout = randomTimeout()

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

	if logLevel >= INFO {
		r.logLocal(fmt.Sprintf("Starting election for term %d", r.persistent.currentTerm))
	}

	// #C3 Reset election timer.
	r.election.Reset(r.electionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), TCPHEARTBEAT*time.Millisecond)

	// #C4 Send RequestVote RPCs to all other servers.
	req := r.conf.RequestVoteFuture(ctx, &pb.RequestVoteRequest{CandidateID: r.id, Term: r.persistent.currentTerm, LastLogTerm: r.logTerm(len(r.persistent.log)), LastLogIndex: uint64(len(r.persistent.log))})

	go func() {
		defer cancel()
		reply, err := req.Get()

		if err != nil {
			r.logLocal(fmt.Sprintf("RequestVote failed = %v", err))

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

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if response.Term > r.persistent.currentTerm {
		r.becomeFollower(response.Term)

		return
	}

	// Ignore late response
	if response.Term < r.persistent.currentTerm {
		return
	}

	// Cont. from startElection(). We have now received a response from Gorums.

	// #C5 If votes received from majority of server: become leader.
	// Make sure we have not stepped down while waiting for replies.
	if r.state == CANDIDATE && response.VoteGranted {
		// We have received at least a quorum of votes.
		// We are the leader for this term. See Raft Paper Figure 2 -> Rules for Servers -> Leaders.
		if logLevel >= INFO {
			r.logLocal(fmt.Sprintf("Elected leader for term %d", r.persistent.currentTerm))
		}

		r.state = LEADER
		r.leader = r.id
		r.seenLeader = true

		for id := range r.nextIndex {
			r.nextIndex[id] = len(r.persistent.log) + 1
		}

		// #L1 Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
		r.heartbeat.Reset(0)

		r.election.Stop()

		return
	}

	// #C7 If election timeout elapses: start new election.
	// This will happened if we don't receive enough replies in time. Or we lose the election but don't see a higher term number.
}

type aeqrpc struct{}
type aenoqrpc struct{}

func (q *aeqrpc) sendRequest(r *Replica) {
	r.Lock()
	defer r.Unlock()

	if logLevel >= DEBUG {
		r.logLocal(fmt.Sprintf("Sending AppendEntries for term %d", r.persistent.currentTerm))
	}

	var buffer bytes.Buffer

LOOP:
	for i := MAXENTRIES; i > 0; i-- {
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

	nextIndex := 0

	// Always send entries for the most up-to-date client.
	for id := range r.nodes {
		next := r.nextIndex[id] - 1

		if next > nextIndex {
			nextIndex = next
		}
	}

	if len(r.persistent.log) > nextIndex {
		maxEntries := int(min(uint64(nextIndex+MAXENTRIES), uint64(len(r.persistent.log))))

		if !r.batch {
			maxEntries = nextIndex + 1
		}

		entries = r.persistent.log[nextIndex:maxEntries]
	}

	if logLevel >= DEBUG {
		r.logLocal(fmt.Sprintf("Sending %d entries", len(entries)))
	}

	req := &pb.AppendEntriesRequest{
		LeaderID:     r.id,
		Term:         r.persistent.currentTerm,
		PrevLogIndex: uint64(nextIndex),
		PrevLogTerm:  r.logTerm(nextIndex),
		CommitIndex:  r.commitIndex,
		Entries:      entries,
	}

	go func(req *pb.AppendEntriesRequest) {
		ctx, cancel := context.WithTimeout(context.Background(), TCPHEARTBEAT*time.Millisecond)
		defer cancel()

		resp, err := r.conf.AppendEntries(ctx, req)

		if err != nil {
			r.logLocal(fmt.Sprintf("AppendEntries failed = %v", err))

			// We can not return if there is a response, i.e., there is replicas that needs updating.
			if resp.AppendEntriesResponse == nil {
				return
			}
		}

		r.aeHandler.handleResponse(r, resp.AppendEntriesResponse)
	}(req)

	r.heartbeat.Reset(r.heartbeatTimeout)
}

func (q *aenoqrpc) sendRequest(r *Replica) {
	r.Lock()
	defer r.Unlock()

	if logLevel >= DEBUG {
		r.logLocal(fmt.Sprintf("Sending AppendEntries for term %d", r.persistent.currentTerm))
	}

	var buffer bytes.Buffer

LOOP:
	for i := MAXENTRIES; i > 0; i-- {
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
	for id, node := range r.nodes {
		entries := []*pb.Entry{}

		nextIndex := r.nextIndex[id] - 1

		if len(r.persistent.log) > nextIndex {
			maxEntries := int(min(uint64(nextIndex+MAXENTRIES), uint64(len(r.persistent.log))))

			if !r.batch {
				maxEntries = nextIndex + 1
			}

			entries = r.persistent.log[nextIndex:maxEntries]
		}

		if logLevel >= DEBUG {
			r.logTo(id, fmt.Sprintf("Sending %d entries", len(entries)))
		}

		req := &pb.AppendEntriesRequest{
			LeaderID:     r.id,
			Term:         r.persistent.currentTerm,
			PrevLogIndex: uint64(nextIndex),
			PrevLogTerm:  r.logTerm(nextIndex),
			CommitIndex:  r.commitIndex,
			Entries:      entries,
		}

		go func(node *pb.Node, req *pb.AppendEntriesRequest) {
			ctx, cancel := context.WithTimeout(context.Background(), TCPHEARTBEAT*time.Millisecond)
			defer cancel()

			resp, err := node.RaftClient.AppendEntries(ctx, req)

			if err != nil {
				r.logTo(r.raftID[node.ID()], fmt.Sprintf("AppendEntries failed = %v", err))

				return
			}
			r.aeHandler.handleResponse(r, resp)
		}(node, req)
	}

	r.heartbeat.Reset(r.heartbeatTimeout)
}

func (q *aeqrpc) handleResponse(r *Replica, response *pb.AppendEntriesResponse) {
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

	if r.state == LEADER {
		if response.Success {
			for _, id := range response.FollowerID {
				r.matchIndex[id] = int(response.MatchIndex)
				r.nextIndex[id] = r.matchIndex[id] + 1
			}

			return
		}

		// If AppendEntries was not successful reset all.
		for id := range r.nodes {
			r.nextIndex[id] = int(response.MatchIndex)
		}
	}
}

func (q *aenoqrpc) handleResponse(r *Replica, response *pb.AppendEntriesResponse) {
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

	if r.state == LEADER {
		if response.Success {
			r.matchIndex[response.FollowerID[0]] = int(response.MatchIndex)
			r.nextIndex[response.FollowerID[0]] = r.matchIndex[response.FollowerID[0]] + 1

			return
		}

		r.nextIndex[response.FollowerID[0]] = int(max(1, response.MatchIndex))
	}
}

func (r *Replica) becomeFollower(term uint64) {
	r.state = FOLLOWER

	if r.persistent.currentTerm != term {
		if logLevel >= INFO {
			r.logLocal(fmt.Sprintf("Become follower as we transition from term %d to %d", r.persistent.currentTerm, term))
		}

		r.persistent.currentTerm = term

		var buffer bytes.Buffer
		buffer.WriteString(fmt.Sprintf(STORETERM, r.persistent.currentTerm))

		if r.persistent.votedFor != NONE {
			r.persistent.votedFor = NONE

			buffer.WriteString(fmt.Sprintf(STOREVOTEDFOR, r.persistent.votedFor))
		}

		// Write to stable storage
		// TODO Assumes successful
		r.save(buffer.String())
	}

	r.election.Reset(r.electionTimeout)
	r.heartbeat.Stop()
}
