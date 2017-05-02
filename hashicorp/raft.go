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
	f   hraft.ApplyFuture
	res chan raft.Result
}

func (f *future) ResultCh() <-chan raft.Result {
	go func() {
		err := f.f.Error()

		if err != nil {
			f.res <- raft.Result{
				Value: err,
			}
			return
		}

		f.res <- raft.Result{
			Index: f.f.Index(),
			Value: f.f.Response(),
		}
	}()

	return f.res
}

// Wrapper wraps a hashicorp/raft.Raft and implements relab/raft.Raft.
type Wrapper struct {
	n      *hraft.Raft
	sm     raft.StateMachine
	logger logrus.FieldLogger
}

func NewRaft(logger logrus.FieldLogger,
	sm raft.StateMachine, cfg *hraft.Config, trans hraft.Transport,
	logs hraft.LogStore, stable hraft.StableStore, snaps hraft.SnapshotStore,
) *Wrapper {
	w := &Wrapper{
		sm:     sm,
		logger: logger,
	}

	node, err := hraft.NewRaft(cfg, w, logs, stable, snaps, trans)

	if err != nil {
		panic(err)
	}

	w.n = node

	return w
}

func (w *Wrapper) ProposeCmd(ctx context.Context, req []byte) (raft.Future, error) {
	deadline, _ := ctx.Deadline()
	timeout := time.Until(deadline)
	f := w.n.Apply(req, timeout)
	ff := &future{f, make(chan raft.Result, 1)}

	return ff, nil
}

func (w *Wrapper) ReadCmd(context.Context, []byte) (raft.Future, error) {
	panic("ReadCmd not implemented")
}

func (w *Wrapper) ProposeConf(ctx context.Context, req *commonpb.ReconfRequest) (raft.Future, error) {
	panic("ProposeConf not implemented")
}

func (w *Wrapper) Apply(logentry *hraft.Log) interface{} {
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
