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
	"strings"
	"time"

	"github.com/relab/raft"
	pb "github.com/relab/raft/raftpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var id = flag.Uint64("id", 0, "server ID")
var cluster = flag.String("cluster", ":9201", "comma separated cluster servers")
var bench = flag.Bool("quiet", false, "Silence log output")
var recover = flag.Bool("recover", false, "Recover from stable storage")
var cpuprofile = flag.String("cpuprofile", "", "Write cpu profile to file")
var slowQuorum = flag.Bool("slowquorum", false, "set quorum size to the number of servers")
var batch = flag.Bool("batch", true, "enable batching")
var qrpc = flag.Bool("qrpc", false, "enable QRPC")

func main() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	if *id == 0 {
		fmt.Print("-id argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	nodes := strings.Split(*cluster, ",")

	if len(nodes) == 0 {
		fmt.Print("-cluster argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if !*qrpc && len(nodes) != 3 {
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

	r, err := raft.NewReplica(&raft.Config{
		ID:         *id,
		Nodes:      nodes,
		Recover:    *recover,
		Batch:      *batch,
		QRPC:       *qrpc,
		SlowQuorum: *slowQuorum,
	})

	if err != nil {
		log.Fatal(err)
	}

	s := grpc.NewServer()
	pb.RegisterRaftServer(s, r)

	l, err := net.Listen("tcp", nodes[*id-1])

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := s.Serve(l)

		if err != nil {
			log.Fatal(err)
		}
	}()

	if *cpuprofile != "" {
		go func() {
			if err := r.Run(); err != nil {
				log.Fatal(err)
			}
		}()

		reader := bufio.NewReader(os.Stdin)
		reader.ReadLine()

		pprof.StopCPUProfile()
	} else {
		if err := r.Run(); err != nil {
			log.Fatal(err)
		}
	}
}
