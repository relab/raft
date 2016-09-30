package main

import (
	"flag"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/relab/raft"
	"github.com/relab/raft/debug"
	"github.com/relab/raft/proto/gorums"

	"google.golang.org/grpc"
)

var verbosity = flag.Int("verbosity", 0, "verbosity")
var nodes Nodes

func init() {
	flag.Var(&nodes, "node", "server address")
}

func main() {
	rand.Seed(time.Now().UnixNano())

	flag.Parse()

	debug.SetVerbosity(*verbosity)

	rs := &raft.Replica{}
	rs.Lock()

	s := grpc.NewServer()
	gorums.RegisterRaftServer(s, rs)

	l, err := net.Listen("tcp", nodes[0])

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := s.Serve(l)

		if err != nil {
			log.Println(err)
		}
	}()

	// Give the server some time to start
	<-time.After(100 * time.Millisecond)

	if err := rs.Init(nodes); err != nil {
		log.Fatal(err)
	}

	rs.Run()
}
