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

// Event types.
const (
	EventCatchup      EventType = 0
	EventFailure      EventType = 1
	EventElection     EventType = 2
	EventPreElection  EventType = 3
	EventBecomeLeader EventType = 4
	EventAddServer    EventType = 5
	EventRemoveServer EventType = 6
	EventCaughtUp     EventType = 7
	EventRemoved      EventType = 8
	EventAdded        EventType = 9
	EventTerminate    EventType = 10
)

var eventName = map[EventType]string{
	0:  "catchup",
	1:  "failure",
	2:  "election",
	3:  "preelection",
	4:  "becomeleader",
	5:  "addserver",
	6:  "removeserver",
	7:  "caughtup",
	8:  "removed",
	9:  "added",
	10: "terminated",
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
