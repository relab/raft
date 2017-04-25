package raftgorums

import (
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	promc "github.com/prometheus/client_golang/prometheus"
)

type raftMetrics struct {
	aereq       metrics.Histogram
	aeres       metrics.Histogram
	rvreq       metrics.Histogram
	rvres       metrics.Histogram
	cmdCommit   metrics.Histogram
	readReqs    metrics.Counter
	writeReqs   metrics.Counter
	reads       metrics.Counter
	writes      metrics.Counter
	leader      metrics.Gauge
	commitIndex metrics.Gauge
}

var rmetrics = &raftMetrics{
	aereq: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "handle_append_entries_request",
		Help:      "Total time spent handling request.",
		MaxAge:    5 * time.Second,
	}, []string{}),
	aeres: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "handle_append_entries_response",
		Help:      "Total time spent handling response.",
		MaxAge:    5 * time.Second,
	}, []string{}),
	rvreq: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "handle_request_vote_request",
		Help:      "Total time spent handling request.",
		MaxAge:    5 * time.Second,
	}, []string{}),
	rvres: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "handle_request_vote_response",
		Help:      "Total time spent handling response.",
		MaxAge:    5 * time.Second,
	}, []string{}),
	cmdCommit: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "commit_client_command",
		Help:      "Total time spent committing client command.",
		MaxAge:    5 * time.Second,
	}, []string{}),
	readReqs: prometheus.NewCounterFrom(promc.CounterOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "read_requests",
		Help:      "Read requests from clients.",
	}, []string{}),
	writeReqs: prometheus.NewCounterFrom(promc.CounterOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "write_requests",
		Help:      "Write requests from clients.",
	}, []string{}),
	reads: prometheus.NewCounterFrom(promc.CounterOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "reads",
		Help:      "Reads processed.",
	}, []string{}),
	writes: prometheus.NewCounterFrom(promc.CounterOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "writes",
		Help:      "Writes processed.",
	}, []string{}),
	leader: prometheus.NewGaugeFrom(promc.GaugeOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "leader",
		Help:      "Current Raft leader.",
	}, []string{}),
	commitIndex: prometheus.NewGaugeFrom(promc.GaugeOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "commit_index",
		Help:      "Current commit index.",
	}, []string{}),
}
