package etcd

import (
	"encoding/binary"
	"net/http"
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
	res chan raft.Result
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

	heartbeat time.Duration

	uid       uint64
	propLock  sync.Mutex
	proposals map[uint64]chan<- raft.Result

	apply chan []raftpb.Entry
}

func (w *Wrapper) newFuture(uid uint64) raft.Future {
	res := make(chan raft.Result, 1)
	future := &future{res}

	w.propLock.Lock()
	w.proposals[uid] = res
	w.propLock.Unlock()

	return future
}

func NewRaft(logger logrus.FieldLogger,
	sm raft.StateMachine, storage *etcdraft.MemoryStorage, wal *wal.WAL, cfg *etcdraft.Config,
	peers []etcdraft.Peer, heartbeat time.Duration,
) *Wrapper {
	w := &Wrapper{
		sm:        sm,
		storage:   storage,
		wal:       wal,
		heartbeat: heartbeat,
		proposals: make(map[uint64]chan<- raft.Result),
		logger:    logger,
		apply:     make(chan []raftpb.Entry, 2048),
	}
	w.n = etcdraft.StartNode(cfg, append(peers, etcdraft.Peer{ID: cfg.ID}))

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
	w.logger.Warnln("ProposeRead")
	return nil, nil
}

func (w *Wrapper) ProposeConf(ctx context.Context, req *commonpb.ReconfRequest) (raft.Future, error) {
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeType(req.ReconfType),
		ID:      req.ServerID, // ?
		NodeID:  req.ServerID,
		Context: []byte(""), // ?
	}

	err := w.n.ProposeConfChange(ctx, cc)

	w.logger.WithError(err).WithFields(logrus.Fields{
		"req": req,
		"cc":  cc,
	}).Warnln("ProposeConf")

	if err != nil {
		return nil, err
	}

	panic("ProposeConf not implemented")
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
				w.propLock.Lock()
				if rd.RaftState != etcdraft.StateLeader && len(w.proposals) > 0 {
					for uid, respCh := range w.proposals {
						respCh <- raft.Result{
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
	ch, ok := w.proposals[uid]
	w.propLock.Unlock()

	if !ok {
		return
	}

	ch <- raft.Result{
		Index: entry.Index,
		Value: res,
	}

	w.propLock.Lock()
	delete(w.proposals, uid)
	w.propLock.Unlock()
}

func (w *Wrapper) handleConfChange(entry *raftpb.Entry) {
	var remove int
	if entry.Data[len(entry.Data)-1] == 0x9 {
		remove = 9
	}
	data := entry.Data[:len(entry.Data)-remove]
	var cc raftpb.ConfChange
	err := cc.Unmarshal(data)

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
			return
		}
		w.transport.RemovePeer(tid)
	}

	// Inform state machine about new configuration.
	w.sm.Apply(&commonpb.Entry{
		Term:      entry.Term,
		Index:     entry.Index,
		EntryType: commonpb.EntryReconf,
		Data:      data,
	})

	// TODO Respond / Cleanup future.
}

func (w *Wrapper) Process(ctx context.Context, m raftpb.Message) error {
	return w.n.Step(ctx, m)
}
func (w *Wrapper) IsIDRemoved(id uint64) bool                               { return false }
func (w *Wrapper) ReportUnreachable(id uint64)                              {}
func (w *Wrapper) ReportSnapshot(id uint64, status etcdraft.SnapshotStatus) {}
