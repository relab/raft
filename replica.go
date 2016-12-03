package raft

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
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
	HEARTBEAT     = 50
	ELECTION      = 150
	APPENDENTRIES = 100
)

// NONE represents no server.
// This must be a value which cannot be returned from idutil.IDFromAddress(address).
const NONE = 0

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

	commitIndex int

	nextIndex  map[uint32]int
	matchIndex map[uint32]int

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	election  Timer
	heartbeat Timer

	recoverFile *os.File
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
			grpc.WithTimeout(time.Second*10)))

	if err != nil {
		return err
	}

	n := len(nodes) + 1

	qspec := &QuorumSpec{
		N: n,
		Q: n / 2,
	}

	conf, err := mgr.NewConfiguration(mgr.NodeIDs(), qspec, time.Second)

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
	log.Println(recoverFile)

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

				if len(split) > 2 {
					return errors.New("Err: Comma in command")
				}

				term, err := strconv.Atoi(split[0])

				if err != nil {
					return err
				}

				entry.Term = uint64(term)
				entry.Data = split[1]

				r.log = append(r.log, &entry)
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
		r.recoverFile.WriteString(fmt.Sprintf("VOTED,%d\n", r.votedFor))

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
				r.recoverFile.WriteString(fmt.Sprintf("%d,%s\n", entry.Term, entry.Data))

				var log []string

				for _, entry := range r.log {
					log = append(log, fmt.Sprintf("%d:%s", entry.Term, entry.Data))
				}

				debug.Debugln(r.id, ":: LOG, len:", len(r.log), "data:", strings.Join(log, ", "))
			}

			r.commitIndex = int(min(request.CommitIndex, uint64(index)))
		}
	}

	return &gorums.AppendEntriesResponse{FollowerID: r.id, Term: r.currentTerm, MatchIndex: uint64(len(r.log)), Success: success}, nil
}

func (r *Replica) ClientCommand(ctx context.Context, request *gorums.ClientRequest) (*gorums.ClientResponse, error) {
	if r.state == LEADER {
		// Append to log, replicate, commit
		// Apply in log order
		// Reply

		debug.Debugln(r.id, ":: CLIENTREQUEST:", string(request.Command))
		r.log = append(r.log, &gorums.Entry{Term: r.currentTerm, Data: request.Command})

		// Write to stable storage
		// TODO Assumes successful
		r.recoverFile.WriteString(fmt.Sprintf("%d,%s\n", r.currentTerm, request.Command))

		return &gorums.ClientResponse{Status: gorums.OK, Response: request.Command}, nil
	}

	var hint uint32

	if r.seenLeader {
		hint = r.leader
	}

	// If client receives hint = 0, it should try another random server.
	return &gorums.ClientResponse{Status: gorums.NOT_LEADER, LeaderHint: hint}, nil
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
	r.recoverFile.WriteString(fmt.Sprintf("TERM,%d\n", r.currentTerm))

	// #C2 Vote for self.
	r.votedFor = r.id

	// Write to stable storage
	// TODO Assumes successful
	r.recoverFile.WriteString(fmt.Sprintf("VOTED,%d\n", r.votedFor))

	debug.Debugln(r.id, ":: ELECTION STARTED, for term", r.currentTerm)

	// #C3 Reset election timer.
	r.election.Reset(r.electionTimeout)

	// #C4 Send RequestVote RPCs to all other servers.
	req := r.conf.RequestVoteFuture(&gorums.RequestVoteRequest{CandidateID: r.id, Term: r.currentTerm, LastLogIndex: r.logTerm(len(r.log)), LastLogTerm: uint64(len(r.log))})

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

	debug.Debugln(r.id, ":: APPENDENTRIES, for term", r.currentTerm)

	// #L1
	for id, node := range r.nodes {
		entries := []*gorums.Entry{}

		nextIndex := r.nextIndex[id] - 1

		if len(r.log) > nextIndex {
			entries = r.log[nextIndex : nextIndex+1]
		}

		req := &gorums.AppendEntriesRequest{
			LeaderID:     r.id,
			Term:         r.currentTerm,
			PrevLogIndex: uint64(nextIndex),
			PrevLogTerm:  r.logTerm(nextIndex),
			Entries:      entries,
		}

		go func(node *gorums.Node, req *gorums.AppendEntriesRequest) {
			ctx, cancel := context.WithTimeout(context.Background(), APPENDENTRIES*time.Millisecond)
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
}

func (r *Replica) handleAppendEntriesResponse(response *gorums.AppendEntriesResponse) {
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

	if r.state == LEADER {
		if response.Success {
			r.matchIndex[response.FollowerID] = int(response.MatchIndex)
			r.nextIndex[response.FollowerID] = r.matchIndex[response.FollowerID] + 1

			return
		}

		r.nextIndex[response.FollowerID] = int(max(0, uint64(r.nextIndex[response.FollowerID]-1)))
	}
}

func (r *Replica) becomeFollower(term uint64) {
	r.state = FOLLOWER

	if r.currentTerm != term {
		debug.Debugln(r.id, ":: STEPDOWN,", r.currentTerm, "->", term)

		r.currentTerm = term

		// Write to stable storage
		// TODO Assumes successful
		r.recoverFile.WriteString(fmt.Sprintf("TERM,%d\n", r.currentTerm))

		if r.votedFor != NONE {
			r.votedFor = NONE

			// Write to stable storage
			// TODO Assumes successful
			r.recoverFile.WriteString(fmt.Sprintf("VOTED,%d\n", r.votedFor))
		}
	}

	r.election.Reset(r.electionTimeout)
	r.heartbeat.Stop()
}
