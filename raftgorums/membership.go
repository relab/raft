package raftgorums

import (
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
	gorums "github.com/relab/raft/raftgorums/gorumspb"
)

type membership struct {
	id     uint64
	mgr    *gorums.Manager
	lookup map[uint64]int
	logger logrus.FieldLogger

	sync.RWMutex
	latest         *gorums.Configuration
	committed      *gorums.Configuration
	latestIndex    uint64
	committedIndex uint64
	pending        *commonpb.ReconfRequest
	stable         bool
	enabled        bool
}

func (m *membership) isActive() bool {
	m.RLock()
	defer m.RUnlock()

	return m.enabled
}

func (m *membership) startReconfiguration(req *commonpb.ReconfRequest) bool {
	m.Lock()
	defer m.Unlock()

	valid := true

	// Disallow servers not available in manager.
	if req.ServerID < 1 || req.ServerID > uint64(len(m.lookup)+1) {
		return false
	}

	switch req.ReconfType {
	case commonpb.ReconfAdd:
		conf, _ := m.addServer(req.ServerID)

		// Disallow configurations that do not result in a change.
		if conf == m.committed {
			valid = false
		}
	case commonpb.ReconfRemove:
		// Disallow configurations that result in a configuration
		// without nodes.
		if len(m.committed.NodeIDs()) == 1 {
			valid = false
			break
		}

		conf, enabled := m.removeServer(req.ServerID)

		// Disallow configurations that do not result in a change, but
		// only if we also don't step down.
		if conf == m.committed && enabled {
			valid = false
		}
	default:
		panic("malformed reconf request")
	}

	m.logger.WithFields(logrus.Fields{
		"pending": m.pending,
		"stable":  m.stable,
		"valid":   valid,
	}).Warnln("Attempt start reconfiguration")

	if m.pending == nil && m.stable && valid {
		m.pending = req
		return true
	}

	return false
}

func (m *membership) setPending(req *commonpb.ReconfRequest) {
	m.Lock()
	m.pending = req
	m.Unlock()
}

func (m *membership) setStable(stable bool) {
	m.Lock()
	// TODO Raft should setStable.
	m.stable = stable
	m.Unlock()
}

func (m *membership) set(index uint64) {
	m.Lock()
	defer m.Unlock()

	switch m.pending.ReconfType {
	case commonpb.ReconfAdd:
		m.latest, m.enabled = m.addServer(m.pending.ServerID)
	case commonpb.ReconfRemove:
		m.latest, m.enabled = m.removeServer(m.pending.ServerID)
	}
	m.latestIndex = index
	m.logger.WithField("latest", m.latest.NodeIDs()).Warnln("New configuration")
}

func (m *membership) commit() bool {
	m.Lock()
	defer m.Unlock()

	m.pending = nil
	m.committed = m.latest
	m.committedIndex = m.latestIndex

	return m.enabled
}

func (m *membership) rollback() {
	m.Lock()
	m.latest = m.committed
	m.latestIndex = m.committedIndex
	m.Unlock()
}

func (m *membership) get() *gorums.Configuration {
	m.RLock()
	defer m.RUnlock()

	return m.latest
}

// TODO Return the same configuration if adding/removing self.

// addServer returns a new configuration including the given server.
func (m *membership) addServer(serverID uint64) (conf *gorums.Configuration, enabled bool) {
	// TODO Clean up.
	if m.enabled {
		enabled = true
	}

	nodeIDs := m.committed.NodeIDs()

	// TODO Not including self in the configuration seems to complicate
	// things. I Foresee a problem when removing the leader, and it is not
	// part of it's own latest configuration.
	if serverID != m.id {
		// Work around bug in Gorums. Duplicated node ids are not deduplicated.
		for _, nodeID := range nodeIDs {
			if nodeID == m.getNodeID(serverID) {
				var err error
				conf, err = m.mgr.NewConfiguration(nodeIDs, NewQuorumSpec(len(nodeIDs)+1))

				if err != nil {
					panic("addServer: " + err.Error())
				}

				return
			}
		}

		id := m.getNodeID(serverID)
		nodeIDs = append(nodeIDs, id)
	} else {
		enabled = true
	}

	// We can ignore the error as we are adding 1 server, and id is
	// guaranteed to be in the manager or getNodeID would have panicked.
	var err error
	conf, err = m.mgr.NewConfiguration(nodeIDs, NewQuorumSpec(len(nodeIDs)+1))

	if err != nil {
		panic("addServer: " + err.Error())
	}

	return
}

// removeServer returns a new configuration excluding the given server.
func (m *membership) removeServer(serverID uint64) (conf *gorums.Configuration, enabled bool) {
	// TODO Clean up.
	enabled = true

	oldIDs := m.committed.NodeIDs()

	if serverID == m.id {
		enabled = false

		var err error
		conf, err = m.mgr.NewConfiguration(oldIDs, NewQuorumSpec(len(oldIDs)+1))

		if err != nil {
			panic("removeServer: " + err.Error())
		}

		return
	}

	id := m.getNodeID(serverID)
	var nodeIDs []uint32

	for _, nodeID := range oldIDs {
		if nodeID == id {
			continue
		}

		nodeIDs = append(nodeIDs, nodeID)
	}

	// We can ignore the error as we do not allow cluster size < 2, and id
	// is guaranteed to be in the manager or getNodeID would have panicked.
	// Cluster size > 2 is a limitation of Gorums and how we have chosen not
	// to include ourselves in the manager.
	var err error
	conf, err = m.mgr.NewConfiguration(nodeIDs, NewQuorumSpec(len(nodeIDs)+1))

	if err != nil {
		panic("removeServer: " + err.Error())
	}

	return
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

func (r *Raft) replicate(serverID uint64, promise raft.PromiseEntry) {
	node := r.mem.getNode(serverID)
	var matchIndex uint64
	var errs int

	for {
		r.Lock()
		target := r.matchIndex
		// TODO We don't need lock on maxAppendEntries as it's only read
		// across all routines.
		maxEntries := r.maxAppendEntries

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
				promise.Respond(&commonpb.ReconfResponse{
					Status: commonpb.ReconfTimeout,
				})
				r.mem.rollback()
				return
			}

			continue
		}

		r.Lock()
		state := r.state
		r.Unlock()

		if state != Leader {
			promise.Respond(&commonpb.ReconfResponse{
				Status: commonpb.ReconfNotLeader,
			})
			r.mem.rollback()
			return
		}

		if target-matchIndex < maxEntries {
			// TODO Context?
			r.queue <- promise
			return
		}

		if res.Success {
			matchIndex = res.MatchIndex
			continue
		}

		matchIndex = max(0, res.MatchIndex)
	}
}
