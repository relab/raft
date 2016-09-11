package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/relab/gorums/idutil"
	"github.com/relab/raft/proto/gorums"
)

var verbose bool

func dlog(v ...interface{}) {
	if verbose {
		log.Println(v)
	}
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	port := flag.String("port", "", "port to run on")
	flag.BoolVar(&verbose, "debug", false, "print debugs loglines")

	var nodes nodeFlags
	flag.Var(&nodes, "node", "raft node")

	flag.Parse()

	id, err := idutil.IDFromAddress(fmt.Sprintf(":%s", *port))

	if err != nil {
		log.Fatal(err)
	}

	rndTimeout := time.Duration(1500+rand.Intn(3000-1500)) * time.Millisecond

	log.Println(id, "timeout:", rndTimeout)

	raftNode := &node{
		id:               id,
		electionTimeout:  rndTimeout,
		heartbeatTimeout: 500 * time.Millisecond,
		heartbeat:        time.NewTimer(0),
		incoming:         make(chan messageIn),
		done:             make(chan struct{}),
	}

	// Drain immediately as we are in the follower state.
	// TODO: Can we create a timer without starting it?
	<-raftNode.heartbeat.C

	server := grpc.NewServer()
	gorums.RegisterRegisterServer(server, raftNode)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := server.Serve(listener)

		log.Println(err)

		close(raftNode.done)
	}()

	// Allow server to start
	<-time.After(time.Millisecond * 4000)

	raftNode.Run(nodes)

	log.Println("FIN")
}
