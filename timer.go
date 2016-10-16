package raft

import "time"

// Timer allows for concurrent Stop and Reset on a timer.
type Timer struct {
	*time.Timer
}

// NewTimer creates, starts and returns a Timer with duration of d.
func NewTimer(d time.Duration) Timer {
	return Timer{time.NewTimer(d)}
}

// Stop safely stops the timer.
func (t Timer) Stop() {
	if !t.Timer.Stop() {
		select {
		case <-t.Timer.C:
		default:
		}
	}
}

// Reset safely resets the timer to a duration of d.
func (t Timer) Reset(d time.Duration) {
	t.Stop()
	t.Timer.Reset(d)
}
