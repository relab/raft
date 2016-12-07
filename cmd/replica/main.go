package main

import (
	"bufio"
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime/pprof"
	"time"

	"github.com/relab/raft"
	"github.com/relab/raft/debug"
	"github.com/relab/raft/proto/gorums"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var verbosity = flag.Int("verbosity", 0, "verbosity level")
var this = flag.String("this", "", "local server address")
var bench = flag.Bool("bench", false, "Silence output for benchmarking")
var recover = flag.Bool("recover", false, "Recover from stable storage")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var nodes raft.Nodes

func init() {
	flag.Var(&nodes, "node", "server address")
}

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

	if len(*this) == 0 {
		log.Fatal("Missing local server address.")
	}

	if len(nodes) == 0 {
		log.Fatal("Missing server addresses.")
	}

	debug.SetVerbosity(*verbosity)

	if *bench {
		debug.SetVerbosity(0)
		log.SetOutput(ioutil.Discard)
		silentLogger := log.New(ioutil.Discard, "", log.LstdFlags)
		grpclog.SetLogger(silentLogger)
		grpc.EnableTracing = false
		rand.Seed(42)
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

	if err := rs.Init(*this, nodes, *recover); err != nil {
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
