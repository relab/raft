package hashicorp

import (
	"fmt"
	"io"
	"time"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	hraft "github.com/hashicorp/raft"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
)

type future struct {
	apply hraft.ApplyFuture
	index hraft.IndexFuture
	res   chan raft.Result
	start time.Time
	lat   *raft.Latency
}

func (f *future) ResultCh() <-chan raft.Result {
	go func() {
		confChange := false
		var g hraft.Future = f.apply
		if g == nil {
			confChange = true
			g = f.index
		}
		err := g.Error()

		if err != nil {
			f.res <- raft.Result{
				Value: err,
			}
			return
		}

		f.lat.Record(f.start)

		if !confChange {
			f.res <- raft.Result{
				Index: f.apply.Index(),
				Value: f.apply.Response(),
			}
			return
		}
		f.res <- raft.Result{
			Index: f.index.Index(),
			Value: &commonpb.ReconfResponse{
				Status: commonpb.ReconfOK,
			},
		}
	}()

	return f.res
}

// Wrapper wraps a hashicorp/raft.Raft and implements relab/raft.Raft.
type Wrapper struct {
	n       *hraft.Raft
	sm      raft.StateMachine
	servers []hraft.Server
	lat     *raft.Latency
	logger  logrus.FieldLogger
}

func NewRaft(logger logrus.FieldLogger,
	sm raft.StateMachine, cfg *hraft.Config, servers []hraft.Server, trans hraft.Transport,
	logs hraft.LogStore, stable hraft.StableStore, snaps hraft.SnapshotStore,
	enabled []uint64,
	lat *raft.Latency,
) *Wrapper {
	w := &Wrapper{
		sm:      sm,
		servers: servers,
		lat:     lat,
		logger:  logger,
	}

	node, err := hraft.NewRaft(cfg, w, logs, stable, snaps, trans)
	if err != nil {
		panic(err)
	}

	voters := make([]hraft.Server, len(enabled))

	for i, id := range enabled {
		voters[i] = servers[id-1]
	}

	f := node.BootstrapCluster(hraft.Configuration{Servers: voters})
	if err := f.Error(); err != nil {
		panic(err)
	}

	w.n = node

	return w
}

func (w *Wrapper) ProposeCmd(ctx context.Context, req []byte) (raft.Future, error) {
	deadline, _ := ctx.Deadline()
	timeout := time.Until(deadline)
	ff := &future{lat: w.lat, start: time.Now(), res: make(chan raft.Result, 1)}
	ff.apply = w.n.Apply(req, timeout)

	return ff, nil
}

func (w *Wrapper) ReadCmd(context.Context, []byte) (raft.Future, error) {
	panic("ReadCmd not implemented")
}

func (w *Wrapper) ProposeConf(ctx context.Context, req *commonpb.ReconfRequest) (raft.Future, error) {
	deadline, _ := ctx.Deadline()
	timeout := time.Until(deadline)
	server := w.servers[req.ServerID-1]
	ff := &future{lat: w.lat, start: time.Now(), res: make(chan raft.Result, 1)}

	switch req.ReconfType {
	case commonpb.ReconfAdd:
		ff.index = w.n.AddVoter(server.ID, server.Address, 0, timeout)
	case commonpb.ReconfRemove:
		ff.index = w.n.RemoveServer(server.ID, 0, timeout)
	default:
		panic("invalid reconf type")
	}

	return ff, nil
}

func (w *Wrapper) Apply(logentry *hraft.Log) interface{} {
	rmetrics.commitIndex.Set(float64(logentry.Index))

	switch logentry.Type {
	case hraft.LogCommand:
		res := w.sm.Apply(&commonpb.Entry{
			Term:      logentry.Term,
			Index:     logentry.Index,
			EntryType: commonpb.EntryNormal,
			Data:      logentry.Data,
		})
		return res
	}

	panic(fmt.Sprintf("no case for logtype: %v", logentry.Type))
}

func (w *Wrapper) Snapshot() (hraft.FSMSnapshot, error) { return &snapStore{}, nil }
func (w *Wrapper) Restore(io.ReadCloser) error          { return nil }

type snapStore struct{}

func (s *snapStore) Persist(sink hraft.SnapshotSink) error { return nil }
func (s *snapStore) Release()                              {}
