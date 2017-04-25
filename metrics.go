package raft

import (
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	promc "github.com/prometheus/client_golang/prometheus"
)

type raftMetrics struct {
	ioread  metrics.Histogram
	iowrite metrics.Histogram
}

var rmetrics = &raftMetrics{
	ioread: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "io_read",
		Help:      "Total time spent reading from disk.",
		MaxAge:    5 * time.Second,
	}, []string{}),
	iowrite: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "server",
		Name:      "io_write",
		Help:      "Total time spent writing to disk.",
		MaxAge:    5 * time.Second,
	}, []string{}),
}
