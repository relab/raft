package main

import (
	"flag"
	"log"
	"math/rand"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/relab/raft"
	"github.com/relab/raft/debug"
	"github.com/relab/raft/proto/gorums"
)

var verbosity = flag.Int("verbosity", 0, "verbosity level")
var nodes raft.Nodes

func init() {
	flag.Var(&nodes, "node", "server address")
}

func main() {
	rand.Seed(time.Now().UnixNano())

	flag.Parse()

	if len(nodes) == 0 {
		log.Fatal("Missing server addresses.")
	}

	debug.SetVerbosity(*verbosity)

	mgr, err := gorums.NewManager(nodes,
		gorums.WithGrpcDialOptions(
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(time.Second*10)))

	if err != nil {
		log.Fatal("Error creating manager:", err)
	}

	replicas := mgr.Nodes(false)

	r := rand.Intn(len(replicas))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	reply, err := replicas[r].RaftClient.RegisterClient(ctx, &gorums.RegisterClientRequest{})

	log.Println(reply, err)
}
