package etcd

import (
	"bytes"
	"encoding/gob"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/types"
	etcdraft "github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
)

type tag struct {
	UID  uint64
	Data []byte
}

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
	storage   *etcdraft.MemoryStorage
	transport *rafthttp.Transport
	logger    logrus.FieldLogger

	heartbeat time.Duration

	uid       uint64
	proposals map[uint64]*future
}

func (w *Wrapper) encodeProposal(data []byte) (uint64, []byte, error) {
	uid := atomic.AddUint64(&w.uid, 1)
	t := &tag{
		UID:  uid,
		Data: data,
	}

	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(t)
	if err != nil {
		return 0, nil, err
	}

	return uid, b.Bytes(), nil
}

func (w *Wrapper) decodeCommit(commit []byte) (*tag, error) {
	b := bytes.NewBuffer(commit)
	dec := gob.NewDecoder(b)

	var t tag
	err := dec.Decode(&t)

	if err != nil {
		return nil, err
	}

	return &t, nil
}

func NewRaft(logger logrus.FieldLogger, storage *etcdraft.MemoryStorage, cfg *etcdraft.Config, peers []etcdraft.Peer, heartbeat time.Duration) *Wrapper {
	w := &Wrapper{
		heartbeat: heartbeat,
		proposals: make(map[uint64]*future),
		storage:   storage,
		logger:    logger,
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

	go w.run()
	return w
}

func (w *Wrapper) Handler() http.Handler {
	return w.transport.Handler()
}

func (w *Wrapper) ProposeCmd(ctx context.Context, req []byte) (raft.Future, error) {
	w.logger.Warnln("ProposeCmd")

	uid, prop, err := w.encodeProposal(req)

	if err != nil {
		return nil, err
	}

	future := &future{make(chan raft.Result, 1)}
	w.proposals[uid] = future

	err = w.n.Propose(ctx, prop)

	if err != nil {
		delete(w.proposals, uid)
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
			w.logger.WithField("rd", rd).Warnln("Ready")
			w.storage.Append(rd.Entries)
			if !etcdraft.IsEmptyHardState(rd.HardState) {
				w.logger.WithField("hardstate", rd.HardState).Warnln("HardState")
				w.storage.SetHardState(rd.HardState)
			}
			if !etcdraft.IsEmptySnap(rd.Snapshot) {
				w.logger.WithField("snapshot", rd.Snapshot).Warnln("Snapshot")
				w.storage.ApplySnapshot(rd.Snapshot)
			}
			w.logger.WithField("messages", rd.Messages).Warnln("Sending")
			w.transport.Send(rd.Messages)
			for _, entry := range rd.CommittedEntries {
				// TODO Decode tag.

				switch entry.Type {
				case raftpb.EntryNormal:
					w.logger.WithField(
						"entry", etcdraft.DescribeEntry(entry, nil),
					).Warnln("Committed normal entry")
					// process(entry)
				case raftpb.EntryConfChange:
					w.logger.WithField(
						"entry", etcdraft.DescribeEntry(entry, nil),
					).Warnln("Committed conf change entry")

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
							return
						}
						w.transport.RemovePeer(tid)
					}
				}

				// TODO Respond to future.
			}
			w.logger.Warnln("Advance")
			w.n.Advance()
		}
	}
}

func (w *Wrapper) Process(ctx context.Context, m raftpb.Message) error {
	w.logger.WithField("m", m).Warnln("Process")
	return w.n.Step(ctx, m)
}
func (w *Wrapper) IsIDRemoved(id uint64) bool                               { return false }
func (w *Wrapper) ReportUnreachable(id uint64)                              {}
func (w *Wrapper) ReportSnapshot(id uint64, status etcdraft.SnapshotStatus) {}
