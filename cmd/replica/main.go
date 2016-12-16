package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime/pprof"
	"time"

	"github.com/relab/raft"
	"github.com/relab/raft/proto/gorums"
	"github.com/relab/raft/rlog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var (
	this       = flag.String("listen", "", "Local server address")
	quiet      = flag.Bool("quiet", false, "Silence log output")
	recover    = flag.Bool("recover", false, "Recover from stable storage")
	cpuprofile = flag.String("cpuprofile", "", "Write cpu profile to file")
	slowQuorum = flag.Bool("slowquorum", false, "set quorum size to the number of servers")
	batch      = flag.Bool("batch", true, "enable batching")
	qrpc       = flag.Bool("qrpc", false, "enable QRPC")
)

var nodes raft.Nodes

func main() {
	rand.Seed(time.Now().UnixNano())

	flag.Var(&nodes, "add", "Remote server address, repeat argument for each server in the cluster")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			die(err)
		}
		pprof.StartCPUProfile(f)
	}

	if len(*this) == 0 {
		fmt.Print("-listen argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if len(nodes) == 0 {
		fmt.Print("-add argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	var logger rlog.Logger

	if *quiet {
		logger = rlog.NewSilentLogger()
		logSink := log.New(ioutil.Discard, "", log.LstdFlags)
		grpclog.SetLogger(logSink)
		grpc.EnableTracing = false
	}

	rs := &raft.Replica{}
	rs.Lock()

	s := grpc.NewServer()
	gorums.RegisterRaftServer(s, rs)

	l, err := net.Listen("tcp", *this)
	if err != nil {
		die(err)
	}

	go func() {
		err := s.Serve(l)
		if err != nil {
			fmt.Print(err)
		}
	}()

	// Wait for the server to start
	<-time.After(500 * time.Millisecond)

	if err := rs.Init(*this, nodes, *recover, *slowQuorum, *batch, *qrpc, logger); err != nil {
		die(err)
	}

	if *cpuprofile != "" {
		go rs.Run()

		reader := bufio.NewReader(os.Stdin)
		reader.ReadLine()

		pprof.StopCPUProfile()
	} else {
		rs.Run()
	}
}

func die(err error) {
	fmt.Print(err)
	os.Exit(1)
}
