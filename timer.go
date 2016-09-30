package raft

import "time"

type Timer struct {
	*time.Timer
}

func NewTimer(d time.Duration) Timer {
	return Timer{time.NewTimer(d)}
}

func (t Timer) Stop() {
	if !t.Timer.Stop() {
		select {
		case <-t.Timer.C:
		default:
		}
	}
}

func (t Timer) Reset(d time.Duration) {
	t.Stop()
	t.Timer.Reset(d)
}
