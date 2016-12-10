package raft

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/relab/gorums/idutil"
	"github.com/relab/raft/debug"
	"github.com/relab/raft/proto/gorums"
)

func trace(s string) (string, time.Time) {
	return s, time.Now()
}

func un(s string, startTime time.Time) {
	endTime := time.Now()
	log.Println("END:", s, "ElapsedTime:", endTime.Sub(startTime))
}

// State represents one of the Raft server states.
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
	// How long we wait for an answer.
	TCPCONNECT   = 5000
	TCPHEARTBEAT = 500

	// Raft RPC timeouts.
	HEARTBEAT = 250
	ELECTION  = 500
)

// NONE represents no server.
// This must be a value which cannot be returned from idutil.IDFromAddress(address).
const NONE = 0

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

func randomTimeout() time.Duration {
	return time.Duration(ELECTION+rand.Intn(ELECTION*2-ELECTION)) * time.Millisecond
}

// Replica represents a Raft server
type Replica struct {
	// Must be acquired before mutating Replica state.
	sync.Mutex

	id       uint32
	leader   uint32
	votedFor uint32

	seenLeader bool

	state State

	conf *gorums.Configuration

	nodes map[uint32]*gorums.Node

	currentTerm uint64

	log []*gorums.Entry

	commitIndex uint64

	nextIndex  map[uint32]int
	matchIndex map[uint32]int

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	election  Timer
	heartbeat Timer

	recoverFile *os.File

	pending  map[uniqueCommand]chan<- *gorums.ClientCommandRequest
	commands map[uniqueCommand]*gorums.ClientCommandRequest

	pendingCount uint64

	lastHeartbeat time.Time
}

type uniqueCommand struct {
	clientID       uint32
	sequenceNumber uint64
}

func (r *Replica) logTerm(index int) uint64 {
	if index < 1 || index > len(r.log) {
		return 0
	}

	return r.log[index-1].Term
}

// Init initializes a Replica.
// This must always be run before Run.
func (r *Replica) Init(this string, nodes []string, recover bool) error {
	defer r.Unlock()

	mgr, err := gorums.NewManager(nodes,
		gorums.WithGrpcDialOptions(
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(TCPCONNECT*time.Millisecond)))

	if err != nil {
		return err
	}

	n := len(nodes) + 1

	qspec := &QuorumSpec{
		N: n,
		Q: n / 2,
	}

	conf, err := mgr.NewConfiguration(mgr.NodeIDs(), qspec, TCPHEARTBEAT*time.Millisecond)

	if err != nil {
		return err
	}

	r.conf = conf

	r.id, err = idutil.IDFromAddress(this)

	if err != nil {
		return err
	}

	r.nodes = make(map[uint32]*gorums.Node, n-1)

	for _, node := range mgr.Nodes(false) {
		r.nodes[node.ID()] = node
	}

	r.electionTimeout = randomTimeout()
	r.heartbeatTimeout = HEARTBEAT * time.Millisecond

	debug.Debugln(r.id, ":: TIMEOUT SET,", r.electionTimeout)

	r.election = NewTimer(r.electionTimeout)
	r.heartbeat = NewTimer(0)
	r.heartbeat.Stop()

	r.votedFor = NONE

	r.nextIndex = make(map[uint32]int, n-1)
	r.matchIndex = make(map[uint32]int, n-1)

	for id := range r.nodes {
		// Initialized to leader last log index + 1.
		r.nextIndex[id] = 1
		r.matchIndex[id] = 0
	}

	recoverFile := fmt.Sprintf("%d", r.id) + ".storage"

	r.pending = make(map[uniqueCommand]chan<- *gorums.ClientCommandRequest)
	r.commands = make(map[uniqueCommand]*gorums.ClientCommandRequest)

	if _, err := os.Stat(recoverFile); !os.IsNotExist(err) && recover {
		file, err := os.Open(recoverFile)

		if err != nil {
			return err
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
					return err
				}

				r.currentTerm = uint64(term)
			case "VOTED":
				votedFor, err := strconv.Atoi(split[1])

				if err != nil {
					return err
				}

				r.votedFor = uint32(votedFor)
			default:
				var entry gorums.Entry

				if len(split) != 4 {
					return errCommaInCommand
				}

				term, err := strconv.Atoi(split[0])

				if err != nil {
					return err
				}

				clientID, err := strconv.Atoi(split[1])

				if err != nil {
					return err
				}

				sequenceNumber, err := strconv.Atoi(split[2])

				if err != nil {
					return err
				}

				command := split[3]

				entry.Term = uint64(term)
				entry.Data = &gorums.ClientCommandRequest{ClientID: uint32(clientID), SequenceNumber: uint64(sequenceNumber), Command: command}

				r.log = append(r.log, &entry)
				r.commands[uniqueCommand{entry.Data.ClientID, entry.Data.SequenceNumber}] = entry.Data
			}
		}

		if err := scanner.Err(); err != nil {
			return err
		}

		// TODO Close?
		r.recoverFile, err = os.OpenFile(recoverFile, os.O_APPEND|os.O_WRONLY, 0666)
	} else {
		// TODO Close?
		r.recoverFile, err = os.Create(recoverFile)
	}

	if err != nil {
		return err
	}

	return nil
}

// Run handles timeouts.
// Always call Init before this method.
// All RPCs are handled by Gorums.
func (r *Replica) Run() {
	r.lastHeartbeat = time.Now()

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

// RequestVote handles a RequestVoteRequest which is invoked by candidates to gather votes.
// See Raft paper § 5.2.
func (r *Replica) RequestVote(ctx context.Context, request *gorums.RequestVoteRequest) (*gorums.RequestVoteResponse, error) {
	r.Lock()
	defer r.Unlock()

	debug.Debugln(r.id, ":: VOTE REQUESTED, from", request.CandidateID, "for term", request.Term, ", my term is", r.currentTerm)

	// #RV1 Reply false if term < currentTerm.
	if request.Term < r.currentTerm {
		return &gorums.RequestVoteResponse{Term: r.currentTerm, RequestTerm: request.Term}, nil
	}

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if request.Term > r.currentTerm {
		r.becomeFollower(request.Term)
	}

	// #RV2 If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
	if (r.votedFor == NONE || r.votedFor == request.CandidateID) &&
		(request.LastLogTerm > r.logTerm(len(r.log)) ||
			(request.LastLogTerm == r.logTerm(len(r.log)) && request.LastLogIndex >= uint64(len(r.log)))) {
		debug.Debugln(r.id, ":: VOTE GRANTED, to", request.CandidateID, "for term", request.Term)

		r.votedFor = request.CandidateID

		// Write to stable storage
		// TODO Assumes successful
		r.save(fmt.Sprintf("VOTED,%d\n", r.votedFor))

		// #F2 If election timeout elapses without receiving AppendEntries RPC from current leader or granting a vote to candidate: convert to candidate.
		// Here we are granting a vote to a candidate so we reset the election timeout.
		r.election.Reset(r.electionTimeout)

		return &gorums.RequestVoteResponse{VoteGranted: true, Term: r.currentTerm, RequestTerm: request.Term}, nil
	}

	// #RV2 The candidate's log was not up-to-date
	return &gorums.RequestVoteResponse{Term: r.currentTerm, RequestTerm: request.Term}, nil
}

// AppendEntries invoked by leader to replicate log entries, also used as a heartbeat.
// See Raft paper § 5.3 and § 5.2.
func (r *Replica) AppendEntries(ctx context.Context, request *gorums.AppendEntriesRequest) (*gorums.AppendEntriesResponse, error) {
	r.Lock()
	defer r.Unlock()
	defer un(trace("APPENDENTRIES"))

	debug.Traceln(r.id, ":: APPENDENTRIES,", request)

	// #AE1 Reply false if term < currentTerm.
	if request.Term < r.currentTerm {
		return &gorums.AppendEntriesResponse{FollowerID: r.id, Success: false, Term: r.currentTerm}, nil
	}

	success := request.PrevLogIndex == 0 || (request.PrevLogIndex-1 < uint64(len(r.log)) && r.log[request.PrevLogIndex-1].Term == request.PrevLogTerm)

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if request.Term > r.currentTerm {
		r.becomeFollower(request.Term)
	} else if r.id != request.LeaderID {
		r.becomeFollower(r.currentTerm)
	}

	if success {
		debug.Debugln(r.id, ":: OK", r.currentTerm)

		r.leader = request.LeaderID
		r.seenLeader = true

		index := int(request.PrevLogIndex)

		for _, entry := range request.Entries {
			index++

			if index == len(r.log) || r.logTerm(index) != entry.Term {
				r.log = r.log[:index-1] // Remove excessive log entries.
				r.log = append(r.log, entry)

				// Write to stable storage
				// TODO Assumes successful
				r.save(fmt.Sprintf("%d,%d,%d,%s\n", entry.Term, entry.Data.ClientID, entry.Data.SequenceNumber, entry.Data.Command))

				debug.Debugln(r.id, ":: LOG, len:", len(r.log))
			}
		}

		old := r.commitIndex

		r.commitIndex = min(request.CommitIndex, uint64(index))

		if r.commitIndex > old {
			r.newCommit(old)
		}
	}

	return &gorums.AppendEntriesResponse{FollowerID: r.id, Term: r.currentTerm, MatchIndex: uint64(len(r.log)), Success: success}, nil
}

func (r *Replica) ClientCommand(ctx context.Context, request *gorums.ClientCommandRequest) (*gorums.ClientCommandResponse, error) {
	defer un(trace("ClientCommand"))

	if response, isLeader := r.logCommand(request); isLeader {
		r.Lock()
		if r.pendingCount >= 50 {
			r.Unlock()
			r.sendAppendEntries()
		} else {
			r.Unlock()
		}

		select {
		// Wait on committed entry.
		case entry := <-response:
			return &gorums.ClientCommandResponse{Status: gorums.OK, Response: entry.Command, ClientID: entry.ClientID}, nil

		// Return if responding takes too much time.
		// The client will retry.
		case <-time.After(TCPHEARTBEAT * time.Millisecond):
			return nil, ErrLateCommit
		}
	}

	hint := r.getHint()

	return &gorums.ClientCommandResponse{Status: gorums.NOT_LEADER, LeaderHint: hint}, nil
}

func (r *Replica) logCommand(request *gorums.ClientCommandRequest) (<-chan *gorums.ClientCommandRequest, bool) {
	r.Lock()
	defer r.Unlock()

	if r.state == LEADER {
		if request.SequenceNumber == 0 {
			debug.Debugln(r.id, ":: REGISTERCLIENT")

			request.ClientID = rand.Uint32()
		} else {
			debug.Debugln(r.id, ":: CLIENTREQUEST:", request.Command)
		}

		if old, ok := r.commands[uniqueCommand{clientID: request.ClientID, sequenceNumber: request.SequenceNumber}]; ok {
			response := make(chan *gorums.ClientCommandRequest, 1)
			response <- old

			return response, true
		}

		r.log = append(r.log, &gorums.Entry{Term: r.currentTerm, Data: request})
		r.pendingCount++

		// Write to stable storage
		// TODO Assumes successful
		r.save(fmt.Sprintf("%d,%d,%d,%s\n", r.currentTerm, request.ClientID, request.SequenceNumber, request.Command))

		response := make(chan *gorums.ClientCommandRequest)

		commandID := uniqueCommand{clientID: request.ClientID, sequenceNumber: request.SequenceNumber}

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
		hint = r.leader
	}

	// If client receives hint = 0, it should try another random server.
	return hint
}

func (r *Replica) advanceCommitIndex() {
	r.Lock()
	defer r.Unlock()

	matchIndexes := []int{len(r.log)}

	for _, i := range r.matchIndex {
		matchIndexes = append(matchIndexes, i)
	}

	sort.Ints(matchIndexes)

	atleast := matchIndexes[(len(r.matchIndex)+1)/2]

	old := r.commitIndex

	if r.state == LEADER && r.logTerm(atleast) == r.currentTerm {
		r.commitIndex = max(r.commitIndex, uint64(atleast))
	}

	if r.commitIndex > old {
		r.newCommit(old)
	}
}

// TODO Assumes caller already holds lock on Replica
func (r *Replica) newCommit(old uint64) {
	for i := old; i < r.commitIndex; i++ {
		committed := r.log[i]
		sequenceNumber := committed.Data.SequenceNumber
		clientID := committed.Data.ClientID
		commandID := uniqueCommand{clientID: clientID, sequenceNumber: sequenceNumber}

		if response, ok := r.pending[commandID]; ok {
			// If entry is not committed fast enough, the client
			// will retry.
			go func(response chan<- *gorums.ClientCommandRequest) {
				select {
				case response <- committed.Data:
				default:
				}
			}(response)
		}

		r.commands[commandID] = committed.Data
	}
}

// TODO Assumes caller already holds lock on Replica
func (r *Replica) save(line string) {
	// TODO benchmark writing to file
	//	r.recoverFile.WriteString(line)
	//	r.recoverFile.Sync()
}

func (r *Replica) startElection() {
	r.Lock()
	defer r.Unlock()

	r.state = CANDIDATE
	r.electionTimeout = randomTimeout()

	// We are now a candidate. See Raft Paper Figure 2 -> Rules for Servers -> Candidates.
	// #C1 Increment currentTerm.
	r.currentTerm++

	// Write to stable storage
	// TODO Assumes successful
	r.save(fmt.Sprintf("TERM,%d\n", r.currentTerm))

	// #C2 Vote for self.
	r.votedFor = r.id

	// Write to stable storage
	// TODO Assumes successful
	r.save(fmt.Sprintf("VOTED,%d\n", r.votedFor))

	debug.Debugln(r.id, ":: ELECTION STARTED, for term", r.currentTerm)

	// #C3 Reset election timer.
	r.election.Reset(r.electionTimeout)

	// #C4 Send RequestVote RPCs to all other servers.
	req := r.conf.RequestVoteFuture(&gorums.RequestVoteRequest{CandidateID: r.id, Term: r.currentTerm, LastLogTerm: r.logTerm(len(r.log)), LastLogIndex: uint64(len(r.log))})

	go func() {
		reply, err := req.Get()
		if err != nil {
			log.Println("startElection: RequestVote error:", err)
			return
		}
		r.handleRequestVoteResponse(reply.Reply)
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
	if r.state == CANDIDATE && response.VoteGranted {
		// We have received at least a quorum of votes.
		// We are the leader for this term. See Raft Paper Figure 2 -> Rules for Servers -> Leaders.

		debug.Debugln(r.id, ":: ELECTED LEADER, for term", r.currentTerm)

		r.state = LEADER
		r.leader = r.id
		r.seenLeader = true

		for id := range r.nextIndex {
			r.nextIndex[id] = len(r.log) + 1
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

	r.pendingCount = 0

	debug.Debugln(r.id, ":: APPENDENTRIES, for term", r.currentTerm)

	// #L1
	for id, node := range r.nodes {
		entries := []*gorums.Entry{}

		nextIndex := r.nextIndex[id] - 1

		if len(r.log) > nextIndex {
			entries = r.log[nextIndex:len(r.log)]
		}

		debug.Debugln("SENDING:", len(entries))

		req := &gorums.AppendEntriesRequest{
			LeaderID:     r.id,
			Term:         r.currentTerm,
			PrevLogIndex: uint64(nextIndex),
			PrevLogTerm:  r.logTerm(nextIndex),
			CommitIndex:  r.commitIndex,
			Entries:      entries,
		}

		go func(node *gorums.Node, req *gorums.AppendEntriesRequest) {
			ctx, cancel := context.WithTimeout(context.Background(), TCPHEARTBEAT*time.Millisecond)
			defer cancel()
			resp, err := node.RaftClient.AppendEntries(ctx, req)

			if err != nil {
				log.Printf("node %d: AppendEntries error: %v", node.ID(), err)
				return
			}
			r.handleAppendEntriesResponse(resp)
		}(node, req)
	}

	r.heartbeat.Reset(r.heartbeatTimeout)

	log.Println("HEARTBEAT ElapsedTime:", time.Now().Sub(r.lastHeartbeat))
	r.lastHeartbeat = time.Now()
}

func (r *Replica) handleAppendEntriesResponse(response *gorums.AppendEntriesResponse) {
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

	if r.state == LEADER {
		if response.Success {
			r.matchIndex[response.FollowerID] = int(response.MatchIndex)
			r.nextIndex[response.FollowerID] = r.matchIndex[response.FollowerID] + 1

			return
		}

		r.nextIndex[response.FollowerID] = int(max(1, uint64(r.nextIndex[response.FollowerID]-1)))
	}
}

func (r *Replica) becomeFollower(term uint64) {
	r.state = FOLLOWER

	if r.currentTerm != term {
		debug.Debugln(r.id, ":: STEPDOWN,", r.currentTerm, "->", term)

		r.currentTerm = term

		// Write to stable storage
		// TODO Assumes successful
		r.save(fmt.Sprintf("TERM,%d\n", r.currentTerm))

		if r.votedFor != NONE {
			r.votedFor = NONE

			// Write to stable storage
			// TODO Assumes successful
			r.save(fmt.Sprintf("VOTED,%d\n", r.votedFor))
		}
	}

	r.election.Reset(r.electionTimeout)
	r.heartbeat.Stop()
}
