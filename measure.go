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

// Catchup is a slice of CSV records.
type Catchup [][]string

// NewCatchup returns a Catchup struct initialized with a header record.
func NewCatchup() *Catchup {
	cat := new(Catchup)
	*cat = append(*cat, []string{"time"})
	return cat
}

// Record records a new CSV record with time set to time.Now().
func (c *Catchup) Record() {
	*c = append(*c, []string{
		fmt.Sprintf("%d", time.Now().UnixNano()),
	})
}

// Write writes all records to a file.
func (c *Catchup) Write(path string) {
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
