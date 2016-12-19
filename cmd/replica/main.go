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

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var this = flag.String("listen", "", "Local server address")
var bench = flag.Bool("quiet", false, "Silence log output")
var recover = flag.Bool("recover", false, "Recover from stable storage")
var cpuprofile = flag.String("cpuprofile", "", "Write cpu profile to file")
var slowQuorum = flag.Bool("slowquorum", false, "set quorum size to the number of servers")
var batch = flag.Bool("batch", true, "enable batching")
var qrpc = flag.Bool("qrpc", false, "enable QRPC")
var nodes raft.Nodes

func main() {
	rand.Seed(time.Now().UnixNano())

	flag.Var(&nodes, "add", "Remote server address, repeat argument for each server in the cluster")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
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

	if !*qrpc && len(nodes) != 2 {
		fmt.Print("only 3 nodes is supported with QRPC enabled\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if *bench {
		log.SetOutput(ioutil.Discard)
		silentLogger := log.New(ioutil.Discard, "", log.LstdFlags)
		grpclog.SetLogger(silentLogger)
		grpc.EnableTracing = false
	}

	rs := &raft.Replica{}
	rs.Lock()

	s := grpc.NewServer()
	gorums.RegisterRaftServer(s, rs)

	l, err := net.Listen("tcp", *this)

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := s.Serve(l)

		if err != nil {
			log.Println(err)
		}
	}()

	// Wait for the server to start
	<-time.After(500 * time.Millisecond)

	if err := rs.Init(*this, nodes, *recover, *slowQuorum, *batch, *qrpc); err != nil {
		log.Fatal(err)
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
