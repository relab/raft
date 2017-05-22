package hashicorp

import (
	"bytes"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/go-msgpack/codec"
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
	id      hraft.ServerID
	n       *hraft.Raft
	sm      raft.StateMachine
	servers []hraft.Server
	conf    hraft.Configuration
	lat     *raft.Latency
	event   *raft.Event
	leader  uint64
	logger  logrus.FieldLogger
}

func NewRaft(logger logrus.FieldLogger,
	sm raft.StateMachine, cfg *hraft.Config, servers []hraft.Server, trans hraft.Transport,
	logs hraft.LogStore, stable hraft.StableStore, snaps hraft.SnapshotStore,
	enabled []uint64,
	lat *raft.Latency, event *raft.Event,
	leaderOut chan struct{},
	id uint64,
	checkQuorum bool,
) *Wrapper {
	w := &Wrapper{
		id:      cfg.LocalID,
		sm:      sm,
		servers: servers,
		lat:     lat,
		event:   event,
		logger:  logger,
	}

	node, err := hraft.NewRaft(cfg, w, logs, stable, snaps, trans, event, checkQuorum)
	if err != nil {
		panic(err)
	}

	voters := make([]hraft.Server, len(enabled))

	for i, id := range enabled {
		voters[i] = servers[id-1]
	}

	w.conf = hraft.Configuration{Servers: voters}

	if servers[id-1].Suffrage == hraft.Voter {
		f := node.BootstrapCluster(w.conf)
		if err := f.Error(); err != nil {
			panic(err)
		}
	}

	w.n = node
	rmetrics.leader.Set(0)

	go func() {
		for {
			if <-node.LeaderCh() {
				atomic.StoreUint64(&w.leader, 1)
				event.Record(raft.EventBecomeLeader)
				rmetrics.leader.Set(float64(id))
				select {
				case leaderOut <- struct{}{}:
					w.logger.Warnln("Sent become leader")
				default:
					w.logger.Warnln("Skipped sending become leader")
				}
			} else {
				atomic.StoreUint64(&w.leader, 0)
				rmetrics.leader.Set(0)
			}
		}
	}()

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
		w.event.Record(raft.EventProposeAddServer)
		ff.index = w.n.AddVoter(server.ID, server.Address, 0, timeout)
	case commonpb.ReconfRemove:
		w.event.Record(raft.EventProposeRemoveServer)
		ff.index = w.n.RemoveServer(server.ID, 0, timeout)
	default:
		panic("invalid reconf type")
	}

	return ff, nil
}

func (w *Wrapper) Apply(logentry *hraft.Log) interface{} {
	rmetrics.commitIndex.Set(float64(logentry.Index))
	if atomic.LoadUint64(&w.leader) != 1 {
		w.lat.Record(time.Now())
	}

	switch logentry.Type {
	case hraft.LogCommand:
		res := w.sm.Apply(&commonpb.Entry{
			Term:      logentry.Term,
			Index:     logentry.Index,
			EntryType: commonpb.EntryNormal,
			Data:      logentry.Data,
		})
		return res
	case hraft.LogConfiguration:
		var configuration hraft.Configuration
		if err := decodeMsgPack(logentry.Data, &configuration); err != nil {
			panic(fmt.Errorf("failed to decode configuration: %v", err))
		}
		// If the server didn't have a vote in the previous conf., but
		// have a vote in the new configuration, this follower have
		// recently been added and are now caught up.
		if !hasVote(w.conf, w.id) && hasVote(configuration, w.id) {
			w.event.Record(raft.EventCaughtUp)
		}
		if len(w.conf.Servers) < len(configuration.Servers) {
			w.event.Record(raft.EventAdded)
		} else {
			w.event.Record(raft.EventRemoved)
		}
		w.conf = configuration
	}

	panic(fmt.Sprintf("no case for logtype: %v", logentry.Type))
}

func (w *Wrapper) Snapshot() (hraft.FSMSnapshot, error) { return &snapStore{}, nil }
func (w *Wrapper) Restore(io.ReadCloser) error          { return nil }

type snapStore struct{}

func (s *snapStore) Persist(sink hraft.SnapshotSink) error { return nil }
func (s *snapStore) Release()                              {}

// Decode reverses the encode operation on a byte slice input.
// From hashicorp/raft/util.go.
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// hasVote returns true if the server identified by 'id' is a Voter in the
// provided Configuration.
// From hashicorp/raft/configuration.go.
func hasVote(configuration hraft.Configuration, id hraft.ServerID) bool {
	for _, server := range configuration.Servers {
		if server.ID == id {
			return server.Suffrage == hraft.Voter
		}
	}
	return false
}
