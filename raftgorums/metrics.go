package raftgorums

import (
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	promc "github.com/prometheus/client_golang/prometheus"
)

type raftMetrics struct {
	aereq     metrics.Histogram
	aeres     metrics.Histogram
	rvreq     metrics.Histogram
	rvres     metrics.Histogram
	cmdCommit metrics.Histogram
}

var rmetrics = &raftMetrics{
	aereq: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "rpc",
		Name:      "handle_append_entries_request",
		Help:      "Total time spent handling request.",
	}, []string{}),
	aeres: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "rpc",
		Name:      "handle_append_entries_response",
		Help:      "Total time spent handling response.",
	}, []string{}),
	rvreq: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "rpc",
		Name:      "handle_request_vote_request",
		Help:      "Total time spent handling request.",
	}, []string{}),
	rvres: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "rpc",
		Name:      "handle_request_vote_response",
		Help:      "Total time spent handling response.",
	}, []string{}),
	cmdCommit: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "internal",
		Name:      "commit_client_command",
		Help:      "Total time spent committing client command.",
	}, []string{}),
}
