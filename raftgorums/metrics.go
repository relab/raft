package raftgorums

import (
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	promc "github.com/prometheus/client_golang/prometheus"
)

type raftMetrics struct {
	aereq metrics.Histogram
}

var rmetrics = &raftMetrics{
	aereq: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "rpc",
		Name:      "handle_append_entries_request",
		Help:      "Total time spent handling request.",
	}, []string{}),
}
