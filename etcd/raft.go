package etcd

import (
	"net/http"
	"strconv"
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

// Wrapper wraps an etcd/raft.Node and implements relab/raft.Raft and
// etcd/rafthttp.Raft.
type Wrapper struct {
	n         etcdraft.Node
	transport *rafthttp.Transport
	logger    logrus.FieldLogger
}

// TODO Use Peer.Context for addr?
func NewRaft(cfg *etcdraft.Config, peers []etcdraft.Peer, addrs []string, logger logrus.FieldLogger) *Wrapper {
	w := &Wrapper{logger: logger}
	w.n = etcdraft.StartNode(cfg, peers)

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
			"addr":    addrs[i],
			"id":      peer.ID,
			"context": string(peer.Context),
		}).Warnln("Add peer")

		transport.AddPeer(types.ID(peer.ID), []string{addrs[i]})
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

	err := w.n.Propose(ctx, req)

	if err != nil {
		return nil, err
	}

	return nil, nil
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

	return nil, nil
}

func (w *Wrapper) run() {
	s := time.NewTicker(5 * time.Millisecond)

	for {
		select {
		case <-s.C:
			w.n.Tick()
		case rd := <-w.n.Ready():
			w.logger.WithField("rd", rd).Warnln("Ready")
			// saveToStorage(rd.State, rd.Entries, rd.Snapshot)
			if !etcdraft.IsEmptySnap(rd.Snapshot) {
				w.logger.WithField("snapshot", rd.Snapshot).Warnln("Snapshot")
				// processSnapshot(rd.Snapshot)
			}
			w.logger.WithField("messages", rd.Messages).Warnln("Sending")
			w.transport.Send(rd.Messages)
			for _, entry := range rd.CommittedEntries {
				switch entry.Type {
				case raftpb.EntryNormal:
					w.logger.WithField("entry", entry).Warnln("Committed normal entry")
					// process(entry)
				case raftpb.EntryConfChange:
					w.logger.WithField("entry", entry).Warnln("Committed conf change entry")
					var cc raftpb.ConfChange
					err := cc.Unmarshal(entry.Data)

					if err != nil {
						panic("unmarshal conf change: " + err.Error())
					}

					w.logger.WithField("cc", cc).Warnln("Applying conf change")
					w.n.ApplyConfChange(cc)
				}
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
