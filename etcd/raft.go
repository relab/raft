package etcd

import (
	"encoding/binary"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/types"
	etcdraft "github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/wal"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
)

type future struct {
	start time.Time
	res   chan raft.Result
}

func (f *future) ResultCh() <-chan raft.Result {
	return f.res
}

// Wrapper wraps an etcd/raft.Node and implements relab/raft.Raft and
// etcd/rafthttp.Raft.
type Wrapper struct {
	n         etcdraft.Node
	sm        raft.StateMachine
	storage   *etcdraft.MemoryStorage
	wal       *wal.WAL
	transport *rafthttp.Transport
	logger    logrus.FieldLogger

	id     uint64
	leader uint64

	lookup   map[uint64][]byte
	confLock sync.Mutex
	conf     uint64

	heartbeat time.Duration

	uid       uint64
	propLock  sync.Mutex
	proposals map[uint64]*future

	apply chan []raftpb.Entry

	lat *raft.Latency
}

func (w *Wrapper) newFuture(uid uint64) raft.Future {
	res := make(chan raft.Result, 1)
	future := &future{time.Now(), res}

	w.propLock.Lock()
	w.proposals[uid] = future
	w.propLock.Unlock()

	return future
}

func NewRaft(logger logrus.FieldLogger,
	sm raft.StateMachine, storage *etcdraft.MemoryStorage, wal *wal.WAL, cfg *etcdraft.Config,
	peers []etcdraft.Peer, heartbeat time.Duration,
	single bool, servers []string,
	lat *raft.Latency,
) *Wrapper {
	w := &Wrapper{
		id:        cfg.ID,
		sm:        sm,
		storage:   storage,
		wal:       wal,
		heartbeat: heartbeat,
		proposals: make(map[uint64]*future),
		logger:    logger,
		apply:     make(chan []raftpb.Entry, 2048),
		lookup:    make(map[uint64][]byte),
		lat:       lat,
	}
	rpeers := append(peers, etcdraft.Peer{ID: cfg.ID})
	if single {
		rpeers = nil
	}
	w.n = etcdraft.StartNode(cfg, rpeers)

	ss := &stats.ServerStats{}
	ss.Initialize()

	transport := &rafthttp.Transport{
		ID:          types.ID(cfg.ID),
		ClusterID:   1,
		Raft:        w,
		ServerStats: ss,
		LeaderStats: stats.NewLeaderStats(strconv.FormatUint(cfg.ID, 10)),
		ErrorC:      make(chan error),
	}
	err := transport.Start()

	if err != nil {
		panic("start transport: " + err.Error())
	}

	for i, addr := range servers {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			w.logger.Fatal(err)
		}
		p, _ := strconv.Atoi(port)

		ur, err := url.Parse("http://" + addr)
		ur.Host = host + ":" + strconv.Itoa(p-100)
		if err != nil {
			w.logger.Fatal(err)
		}
		w.lookup[uint64(i+1)] = []byte(ur.String())
	}

	for i, peer := range peers {
		w.logger.WithFields(logrus.Fields{
			"i":       i,
			"id":      peer.ID,
			"context": string(peer.Context),
		}).Warnln("Add peer")

		transport.AddPeer(types.ID(peer.ID), []string{string(peer.Context)})
	}

	w.transport = transport

	go w.runSM()
	go w.run()
	return w
}

func (w *Wrapper) Handler() http.Handler {
	return w.transport.Handler()
}

func (w *Wrapper) ProposeCmd(ctx context.Context, req []byte) (raft.Future, error) {
	if atomic.LoadUint64(&w.leader) != w.id {
		return nil, errors.New("not leader")
	}

	uid := atomic.AddUint64(&w.uid, 1)
	b := make([]byte, 9)
	binary.LittleEndian.PutUint64(b, uid)
	b[8] = 0x9
	req = append(req, b...)

	future := w.newFuture(uid)

	if err := w.n.Propose(ctx, req); err != nil {
		w.propLock.Lock()
		delete(w.proposals, uid)
		w.propLock.Unlock()
		return nil, err
	}

	return future, nil
}

func (w *Wrapper) ReadCmd(context.Context, []byte) (raft.Future, error) {
	panic("ReadCmd not implemented")
}

func (w *Wrapper) ProposeConf(ctx context.Context, req *commonpb.ReconfRequest) (raft.Future, error) {
	if atomic.LoadUint64(&w.leader) != w.id {
		return nil, errors.New("not leader")
	}

	w.propLock.Lock()
	if w.conf != 0 {
		return nil, errors.New("conf in progress")
	}
	w.propLock.Unlock()

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeType(req.ReconfType),
		ID:      req.ServerID,
		NodeID:  req.ServerID,
		Context: w.lookup[req.ServerID],
	}

	err := w.n.ProposeConfChange(ctx, cc)

	w.logger.WithError(err).WithFields(logrus.Fields{
		"req": req,
		"cc":  cc,
	}).Warnln("ProposeConf")

	if err != nil {
		return nil, err
	}

	w.propLock.Lock()
	w.conf = atomic.AddUint64(&w.uid, 1)
	w.propLock.Unlock()

	future := w.newFuture(w.conf)
	return future, nil
}

func (w *Wrapper) run() {
	s := time.NewTicker(w.heartbeat)

	for {
		select {
		case <-s.C:
			w.n.Tick()
		case rd := <-w.n.Ready():
			if rd.SoftState != nil {
				rmetrics.leader.Set(float64(rd.Lead))
				if w.id == rd.Lead {
					atomic.StoreUint64(&w.leader, rd.Lead)
				}
				w.propLock.Lock()
				if rd.RaftState != etcdraft.StateLeader && len(w.proposals) > 0 {
					for uid, future := range w.proposals {
						future.res <- raft.Result{
							Value: raft.ErrNotLeader{Leader: rd.Lead},
						}
						delete(w.proposals, uid)
					}
				}
				w.propLock.Unlock()
			}
			w.wal.Save(rd.HardState, rd.Entries)
			w.storage.Append(rd.Entries)
			if !etcdraft.IsEmptyHardState(rd.HardState) {
				w.storage.SetHardState(rd.HardState)
			}
			if !etcdraft.IsEmptySnap(rd.Snapshot) {
				w.storage.ApplySnapshot(rd.Snapshot)
			}
			w.n.Advance()
			w.transport.Send(rd.Messages)
			w.apply <- rd.CommittedEntries
		}
	}
}

func (w *Wrapper) runSM() {
	for entries := range w.apply {
		for _, entry := range entries {
			rmetrics.commitIndex.Set(float64(entry.Index))

			switch entry.Type {
			case raftpb.EntryNormal:
				w.handleNormal(&entry)
			case raftpb.EntryConfChange:
				w.logger.WithField(
					"entry", etcdraft.DescribeEntry(entry, nil),
				).Warnln("Committed conf change entry")

				w.handleConfChange(&entry)
			}
		}
	}
}

func (w *Wrapper) handleNormal(entry *raftpb.Entry) {
	if len(entry.Data) == 0 {
		return
	}

	uid := binary.LittleEndian.Uint64(entry.Data[len(entry.Data)-9 : len(entry.Data)-1])
	data := entry.Data[:len(entry.Data)-9]

	res := w.sm.Apply(&commonpb.Entry{
		Term:      entry.Term,
		Index:     entry.Index,
		EntryType: commonpb.EntryNormal,
		Data:      data,
	})

	w.propLock.Lock()
	future, ok := w.proposals[uid]
	delete(w.proposals, uid)
	w.propLock.Unlock()

	if !ok {
		return
	}

	w.lat.Record(future.start)
	future.res <- raft.Result{
		Index: entry.Index,
		Value: res,
	}
}

func (w *Wrapper) handleConfChange(entry *raftpb.Entry) {
	var cc raftpb.ConfChange
	err := cc.Unmarshal(entry.Data)

	if err != nil {
		panic("unmarshal conf change: " + err.Error())
	}

	w.logger.WithField("cc", cc).Warnln("Applying conf change")
	w.n.ApplyConfChange(cc)
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if len(cc.Context) > 0 {
			w.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
		}
	case raftpb.ConfChangeRemoveNode:
		tid := types.ID(cc.NodeID)
		if tid == w.transport.ID {
			w.logger.Warnln("Shutting down")
			os.Exit(0)
		}
		if w.transport.Get(tid) != nil {
			w.transport.RemovePeer(tid)
		}
	}

	// Inform state machine about new configuration.
	w.sm.Apply(&commonpb.Entry{
		Term:      entry.Term,
		Index:     entry.Index,
		EntryType: commonpb.EntryReconf,
		Data:      entry.Data,
	})

	w.propLock.Lock()
	future, ok := w.proposals[w.conf]
	delete(w.proposals, w.conf)
	w.conf = 0
	w.propLock.Unlock()

	if !ok {
		return
	}

	w.lat.Record(future.start)
	future.res <- raft.Result{
		Index: entry.Index,
		Value: &commonpb.ReconfResponse{
			Status: commonpb.ReconfOK,
		},
	}

}

func (w *Wrapper) Process(ctx context.Context, m raftpb.Message) error {
	return w.n.Step(ctx, m)
}
func (w *Wrapper) IsIDRemoved(id uint64) bool                               { return false }
func (w *Wrapper) ReportUnreachable(id uint64)                              {}
func (w *Wrapper) ReportSnapshot(id uint64, status etcdraft.SnapshotStatus) {}
