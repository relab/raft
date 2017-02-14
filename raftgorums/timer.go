package raftgorums

import "time"

// Timer is similar to time.NewTimer however it supports being disabled,
// restarted and can be made to timeout immediately.
type Timer struct {
	C       <-chan time.Time
	timer   *time.Timer
	timeout time.Duration
}

// NewTimer returns a Timer initialized to send on C after the specified
// duration.
func NewTimer(timeout time.Duration) *Timer {
	timer := time.NewTimer(timeout)

	return &Timer{
		C:       timer.C,
		timer:   timer,
		timeout: timeout,
	}
}

// Disable disables any read on C until either Timeout or Restart is called.
func (rt *Timer) Disable() {
	rt.stop()
	rt.C = nil
}

// Timeout causes an immediate send on C.
func (rt *Timer) Timeout() {
	rt.stop()
	timer := time.NewTimer(0)
	rt.timer = timer
	rt.C = timer.C
}

// Restart causes a send on C after timeout duration.
func (rt *Timer) Restart() {
	rt.stop()
	timer := time.NewTimer(rt.timeout)
	rt.timer = timer
	rt.C = timer.C
}

// Should allow faster garbage collection.
func (rt *Timer) stop() {
	if rt.timer != nil {
		rt.timer.Stop()
		rt.timer = nil
	}
}
