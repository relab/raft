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

func (m *membership) reconfigure(conf *gorums.Configuration) {

}

func (r *Raft) replicate(serverID uint64, future *raft.EntryFuture) {
	defer func() {
	}()

	nodeID := r.mem.getNodeID(serverID)
	node, found := r.mem.mgr.Node(nodeID)

	if !found {
		return
	}

	matchIndex := r.matchIndex

	for {
		// TODO Can't do close enough here due to setting matchIndex to
		// r.matchIndex above.

		r.Lock()
		entries := r.getNextEntries(matchIndex)
		res, err := node.RaftClient.AppendEntries(
			context.TODO(),
			r.getAppendEntriesRequest(matchIndex+1, entries),
		)
		r.Unlock()

		if err != nil {
			// TODO handle
		}

		r.Lock()
		if r.state != Leader || res.Term > r.currentTerm {
			// TODO Become follower? Or should we assume that we are
			// informed otherwise?
			return
		}
		r.Unlock()

		if res.Success {
			matchIndex = res.MatchIndex
			// TODO Close enough?
			// r.addServer(serverID)
			// send request on queue
			// sendAppendEntries should set latest
			continue
		}
		matchIndex = max(0, res.MatchIndex)
	}
}
