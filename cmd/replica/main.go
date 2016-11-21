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

var verbosity = flag.Int("verbosity", 0, "verbosity level")
var this = flag.String("this", "", "local server address")
var nodes raft.Nodes

func init() {
	flag.Var(&nodes, "node", "server address")
}

func main() {
	rand.Seed(time.Now().UnixNano())

	flag.Parse()

	if len(*this) == 0 {
		log.Fatal("Missing local server address.")
	}

	if len(nodes) == 0 {
		log.Fatal("Missing server addresses.")
	}

	debug.SetVerbosity(*verbosity)

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

	if err := rs.Init(*this, nodes); err != nil {
		log.Fatal(err)
	}

	rs.Run()
}
