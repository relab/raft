package etcd

import (
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	promc "github.com/prometheus/client_golang/prometheus"
)

type raftMetrics struct {
	leader      metrics.Gauge
	commitIndex metrics.Gauge
}

var rmetrics = &raftMetrics{
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
