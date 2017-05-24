package raft

import (
	"encoding/csv"
	"fmt"
	"os"
	"time"
)

// Latency is a slice of CSV records.
type Latency [][]string

// NewLatency returns a Latency struct initialized with a header record.
func NewLatency() *Latency {
	lat := new(Latency)
	*lat = append(*lat, []string{"start", "end"})
	return lat
}

// Record records a new CSV record with start as the start time and time.Now()
// as the end time.
func (l *Latency) Record(start time.Time) {
	now := time.Now()
	*l = append(*l, []string{
		fmt.Sprintf("%d", start.UnixNano()),
		fmt.Sprintf("%d", now.UnixNano()),
	})
}

// Write writes all records to a file.
func (l *Latency) Write(path string) {
	f, err := os.Create(path)

	if err != nil {
		panic("error creating file: " + err.Error())
	}

	w := csv.NewWriter(f)
	w.WriteAll(*l) // Checking error below.

	if err := w.Error(); err != nil {
		panic("error writing csv: " + err.Error())
	}
}

// EventType is the types of event that Event can record.
type EventType int

const (
	// EventCatchup a follower invoked a catchup.
	EventCatchup EventType = 0
	// EventFailure a server suspected another server of failing.
	EventFailure EventType = 1
	// EventElection an election was initiated.
	EventElection EventType = 2
	// EventPreElection a pre-election was initiated.
	EventPreElection EventType = 3
	// EventBecomeLeader a candidate won an election.
	EventBecomeLeader EventType = 4
	// EventProposeAddServer a leader received a add server request.
	EventProposeAddServer EventType = 5
	// EventProposeRemoveServer a leader received a remove server request.
	EventProposeRemoveServer EventType = 6
	// EventCaughtUp indicates that the a server has caught up to the point
	// where it has applied the configuration change which added it to the
	// cluster.
	EventCaughtUp EventType = 7
	// EventRemoved the remove server request was committed.
	EventRemoved EventType = 8
	// EventAdded the add server request was committed.
	EventAdded EventType = 9
	// EventApplyConfiguration a new configuration is now being used.
	EventApplyConfiguration EventType = 10
	// EventTerminated a server received a termination signal.
	EventTerminated EventType = 11
	// EventStartReplicate the leader started replicating entries to a
	// server.
	EventStartReplicate = 12
	// EventInjectEntries the leader responds to a catchup request by
	// injecting the missing entries in the next request.
	EventInjectEntries = 13
)

var eventName = map[EventType]string{
	0:  "catchup",
	1:  "failure",
	2:  "election",
	3:  "preelection",
	4:  "becomeleader",
	5:  "proposeaddserver",
	6:  "proposeremoveserver",
	7:  "caughtup",
	8:  "removed",
	9:  "added",
	10: "applyconfiguration",
	11: "terminated",
	12: "startreplicate",
	13: "injectentries",
}

// Event is a slice of CSV records.
type Event [][]string

// NewEvent returns a Event struct initialized with a header record.
func NewEvent() *Event {
	e := new(Event)
	*e = append(*e, []string{"event", "time"})
	return e
}

// Record records a new CSV record with time set to time.Now().
func (e *Event) Record(event EventType) {
	*e = append(*e, []string{
		fmt.Sprintf("%s", eventName[event]),
		fmt.Sprintf("%d", time.Now().UnixNano()),
	})
}

// Write writes all records to a file.
func (e *Event) Write(path string) {
	f, err := os.Create(path)

	if err != nil {
		panic("error creating file: " + err.Error())
	}

	w := csv.NewWriter(f)
	w.WriteAll(*e) // Checking error below.

	if err := w.Error(); err != nil {
		panic("error writing csv: " + err.Error())
	}
}

// CatchupRecorder is a slice of CSV records.
type CatchupRecorder [][]string

// NewCatchupRecorder returns a CatchupRecorder struct initialized with a header record.
func NewCatchupRecorder() *CatchupRecorder {
	lat := new(CatchupRecorder)
	*lat = append(*lat, []string{"from", "to", "entries", "discarded", "time"})
	return lat
}

// Record records a new CSV record with time set to time.Now().
func (c *CatchupRecorder) Record(to, from uint64, numEntries int, discarded bool) {
	*c = append(*c, []string{
		fmt.Sprintf("%d", to),
		fmt.Sprintf("%d", from),
		fmt.Sprintf("%d", numEntries),
		fmt.Sprintf("%t", discarded),
		fmt.Sprintf("%d", time.Now().UnixNano()),
	})
}

// Write writes all records to a file.
func (c *CatchupRecorder) Write(path string) {
	f, err := os.Create(path)

	if err != nil {
		panic("error creating file: " + err.Error())
	}

	w := csv.NewWriter(f)
	w.WriteAll(*c) // Checking error below.

	if err := w.Error(); err != nil {
		panic("error writing csv: " + err.Error())
	}
}
