package dev

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/trace"
)

// Manager manages a pool of node configurations on which quorum remote
// procedure calls can be made.
type Manager struct {
	sync.Mutex
	nodes    map[uint32]*Node
	configs  map[uint32]*Configuration
	eventLog trace.EventLog

	closeOnce sync.Once
	logger    *log.Logger
	opts      managerOptions
}

// NewManager attempts to connect to the given set of node addresses and if
// successful returns a new Manager containing connections to those nodes.
func NewManager(nodeAddrs []string, opts ...ManagerOption) (*Manager, error) {
	if len(nodeAddrs) == 0 {
		return nil, fmt.Errorf("could not create manager: no nodes provided")
	}

	m := &Manager{
		nodes:   make(map[uint32]*Node),
		configs: make(map[uint32]*Configuration),
	}

	for _, opt := range opts {
		opt(&m.opts)
	}

	selfAddrIndex, selfID, err := m.parseSelfOptions(nodeAddrs)
	if err != nil {
		return nil, ManagerCreationError(err)
	}

	idSeen := false
	for i, naddr := range nodeAddrs {
		node, err2 := m.createNode(naddr)
		if err2 != nil {
			return nil, ManagerCreationError(err2)
		}
		m.nodes[node.id] = node
		if i == selfAddrIndex {
			node.self = true
			continue
		}
		if node.id == selfID {
			node.self = true
			idSeen = true
		}
	}
	if selfID != 0 && !idSeen {
		return nil, ManagerCreationError(
			fmt.Errorf("WithSelfID provided, but no node with id %d found", selfID),
		)
	}

	if m.opts.trace {
		title := strings.Join(nodeAddrs, ",")
		m.eventLog = trace.NewEventLog("gorums.Manager", title)
	}

	err = m.connectAll()
	if err != nil {
		return nil, ManagerCreationError(err)
	}

	if m.opts.logger != nil {
		m.logger = m.opts.logger
	}

	if m.eventLog != nil {
		m.eventLog.Printf("ready")
	}

	return m, nil
}

func (m *Manager) parseSelfOptions(addrs []string) (int, uint32, error) {
	if m.opts.selfAddr != "" && m.opts.selfID != 0 {
		return 0, 0, fmt.Errorf("both WithSelfAddr and WithSelfID provided")
	}
	if m.opts.selfID != 0 {
		return -1, m.opts.selfID, nil
	}
	if m.opts.selfAddr == "" {
		return -1, 0, nil
	}

	seen, index := contains(m.opts.selfAddr, addrs)
	if !seen {
		return 0, 0, fmt.Errorf(
			"option WithSelfAddr provided, but address %q was not present in address list",
			m.opts.selfAddr)
	}

	return index, 0, nil
}

func (m *Manager) createNode(addr string) (*Node, error) {
	m.Lock()
	defer m.Unlock()

	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("create node %s error: %v", addr, err)
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(tcpAddr.String()))
	id := h.Sum32()

	if _, found := m.nodes[id]; found {
		return nil, fmt.Errorf("create node %s error: node already exists", addr)
	}

	node := &Node{
		id:      id,
		addr:    tcpAddr.String(),
		latency: -1 * time.Second,
	}

	return node, nil
}

func (m *Manager) connectAll() error {
	if m.opts.noConnect {
		return nil
	}

	if m.eventLog != nil {
		m.eventLog.Printf("connecting")
	}

	for _, node := range m.nodes {
		if node.self {
			continue
		}
		err := node.connect(m.opts.grpcDialOpts...)
		if err != nil {
			if m.eventLog != nil {
				m.eventLog.Errorf("connect failed, error connecting to node %s, error: %v", node.addr, err)
			}
			return fmt.Errorf("connect node %s error: %v", node.addr, err)
		}
	}
	return nil
}

func (m *Manager) closeNodeConns() {
	for _, node := range m.nodes {
		if node.self {
			continue
		}
		err := node.close()
		if err == nil {
			continue
		}
		if m.logger != nil {
			m.logger.Printf("node %d: error closing: %v", node.id, err)
		}
	}
}

// Close closes all node connections and any client streams.
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		if m.eventLog != nil {
			m.eventLog.Printf("closing")
		}
		m.closeNodeConns()
	})
}

// NodeIDs returns the identifier of each available node.
func (m *Manager) NodeIDs() []uint32 {
	m.Lock()
	defer m.Unlock()
	ids := make([]uint32, 0, len(m.nodes))
	for id := range m.nodes {
		ids = append(ids, id)
	}
	sort.Sort(idSlice(ids))
	return ids
}

// Node returns the node with the given identifier if present.
func (m *Manager) Node(id uint32) (node *Node, found bool) {
	m.Lock()
	defer m.Unlock()
	node, found = m.nodes[id]
	return node, found
}

// Nodes returns a slice of each available node.
func (m *Manager) Nodes(excludeSelf bool) []*Node {
	m.Lock()
	defer m.Unlock()
	var nodes []*Node
	for _, node := range m.nodes {
		if excludeSelf && node.self {
			continue
		}
		nodes = append(nodes, node)
	}
	OrderedBy(ID).Sort(nodes)
	return nodes
}

// ConfigurationIDs returns the identifier of each available
// configuration.
func (m *Manager) ConfigurationIDs() []uint32 {
	m.Lock()
	defer m.Unlock()
	ids := make([]uint32, 0, len(m.configs))
	for id := range m.configs {
		ids = append(ids, id)
	}
	sort.Sort(idSlice(ids))
	return ids
}

// Configuration returns the configuration with the given global
// identifier if present.
func (m *Manager) Configuration(id uint32) (config *Configuration, found bool) {
	m.Lock()
	defer m.Unlock()
	config, found = m.configs[id]
	return config, found
}

// Configurations returns a slice of each available configuration.
func (m *Manager) Configurations() []*Configuration {
	m.Lock()
	defer m.Unlock()
	configs := make([]*Configuration, 0, len(m.configs))
	for _, conf := range m.configs {
		configs = append(configs, conf)
	}
	return configs
}

// Size returns the number of nodes and configurations in the Manager.
func (m *Manager) Size() (nodes, configs int) {
	m.Lock()
	defer m.Unlock()
	return len(m.nodes), len(m.configs)
}

// AddNode attempts to dial to the provide node address. The node is
// added to the Manager's pool of nodes if a connection was established.
func (m *Manager) AddNode(addr string) error {
	panic("not implemented")
}

// NewConfiguration returns a new configuration given quorum specification and
// a timeout.
func (m *Manager) NewConfiguration(ids []uint32, qspec QuorumSpec) (*Configuration, error) {
	m.Lock()
	defer m.Unlock()

	if len(ids) == 0 {
		return nil, IllegalConfigError("need at least one node")
	}

	var cnodes []*Node
	for _, nid := range ids {
		node, found := m.nodes[nid]
		if !found {
			return nil, NodeNotFoundError(nid)
		}
		if node.self && m.selfSpecified() {
			return nil, IllegalConfigError(
				fmt.Sprintf("self (%d) can't be part of a configuration when a self-option is provided", nid),
			)
		}
		cnodes = append(cnodes, node)
	}

	// Node ids are sorted ensure a globally consistent configuration id.
	OrderedBy(ID).Sort(cnodes)

	h := fnv.New32a()
	for _, node := range cnodes {
		binary.Write(h, binary.LittleEndian, node.id)
	}
	cid := h.Sum32()

	conf, found := m.configs[cid]
	if found {
		return conf, nil
	}

	c := &Configuration{
		id:    cid,
		nodes: cnodes,
		n:     len(cnodes),
		mgr:   m,
		qspec: qspec,
	}
	m.configs[cid] = c

	return c, nil
}

func (m *Manager) selfSpecified() bool {
	return m.opts.selfAddr != "" || m.opts.selfID != 0
}

type idSlice []uint32

func (p idSlice) Len() int           { return len(p) }
func (p idSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p idSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
