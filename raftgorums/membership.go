package raftgorums

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/relab/raft"
	gorums "github.com/relab/raft/raftgorums/gorumspb"
)

type membership struct {
	mgr *gorums.Manager

	latest    *gorums.Configuration
	committed *gorums.Configuration

	latestIndex    uint64
	committedIndex uint64

	lookup map[uint64]int
}

// addServer returns a new configuration including the given server.
func (m *membership) addServer(serverID uint64) (*gorums.Configuration, error) {
	id := m.getNodeID(serverID)
	nodeIDs := append(m.committed.NodeIDs(), id)

	conf, err := m.mgr.NewConfiguration(nodeIDs, NewQuorumSpec(len(nodeIDs)+1))

	if err != nil {
		return nil, err
	}

	return conf, nil
}

// removeServer returns a new configuration excluding the given server.
func (m *membership) removeServer(serverID uint64) (*gorums.Configuration, error) {
	id := m.getNodeID(serverID)
	oldIDs := m.committed.NodeIDs()
	var nodeIDs []uint32

	for _, nodeID := range oldIDs {
		if nodeID == id {
			continue
		}

		nodeIDs = append(nodeIDs, nodeID)
	}

	conf, err := m.mgr.NewConfiguration(nodeIDs, NewQuorumSpec(len(nodeIDs)+1))

	if err != nil {
		return nil, err
	}

	return conf, nil
}

func (m *membership) getNodeID(serverID uint64) uint32 {
	nodeID, ok := m.lookup[serverID]

	if !ok {
		panic(fmt.Sprintf("no lookup available for server %d", serverID))
	}

	return m.mgr.NodeIDs()[nodeID]
}

func (m *membership) getNode(serverID uint64) *gorums.Node {
	// Can ignore error because we looked up the node through the manager
	// first, therefore it exists.
	node, _ := m.mgr.Node(m.getNodeID(serverID))
	return node
}

func (r *Raft) allowReconfiguration() bool {
	return true
}

func (r *Raft) replicate(serverID uint64, future *raft.EntryFuture) {
	node := r.mem.getNode(serverID)
	var matchIndex uint64
	var errs int

	for {
		r.Lock()
		target := r.matchIndex
		maxEntries := r.maxAppendEntries
		r.Unlock()

		if target-matchIndex < maxEntries {
			// TODO r.addServer(serverID) -> send request on queue,
			// modify aereqout to contain new configuration.
			return
		}

		r.Lock()
		entries := r.getNextEntries(matchIndex + 1)
		req := r.getAppendEntriesRequest(matchIndex+1, entries)
		r.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), r.electionTimeout)
		res, err := node.RaftClient.AppendEntries(ctx, req)
		cancel()

		// TODO handle better.
		if err != nil {
			errs++

			if errs > 3 {
				return
			}
		}

		r.Lock()
		state := r.state
		term := r.currentTerm
		r.Unlock()

		if state != Leader || res.Term > term {
			// TODO Become follower? Or should we assume that we are
			// informed otherwise?
			return
		}

		if res.Success {
			matchIndex = res.MatchIndex
			continue
		}

		matchIndex = max(0, res.MatchIndex)
	}
}
