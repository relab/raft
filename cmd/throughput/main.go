package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	gorums "github.com/relab/raft/pkg/raft/gorumspb"
	pb "github.com/relab/raft/pkg/raft/raftpb"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var client gorums.RaftClient

var counter chan interface{}
var seq chan uint64

var leader = flag.String("leader", "", "Leader server address")
var clients = flag.Int("clients", 1, "Number of clients")
var rate = flag.Int("rate", 40, "How often each client sends a request in microseconds")
var timeout = flag.Duration("time", time.Second*30, "How long to measure in `seconds`\n\ttime/2 seconds will be spent to saturate the cluster")

func main() {
	flag.Parse()

	if *leader == "" {
		fmt.Print("-leader argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	t := *timeout

	counter = make(chan interface{})
	seq = make(chan uint64)
	stop := make(chan interface{})
	reset := make(chan interface{})

	go func() {
		i := uint64(1)

		for {
			select {
			case seq <- i:
			case <-stop:
				return
			}

			i++
		}
	}()

	mgr, err := gorums.NewManager([]string{*leader},
		gorums.WithGrpcDialOptions(
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithTimeout(time.Second),
		))

	if err != nil {
		log.Fatal(err)
	}

	client = mgr.Nodes()[0].RaftClient

	count := 0

	go func() {
		for {
			select {
			case <-reset:
				log.Println("Beginning count after:", t/2)
				count = 0
			case <-counter:
				count++
			case <-stop:
				return
			}
		}
	}()

	n := *clients
	wait := time.Duration(*rate) * time.Microsecond

	var wg sync.WaitGroup
	wg.Add(n)

	go func() {
		wg.Wait()

		log.Println("Waiting:", t/2)
		<-time.After(t / 2)

		reset <- struct{}{}
		reset <- struct{}{}
	}()

	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		reply, err := client.ClientCommand(ctx, &pb.ClientCommandRequest{Command: "REGISTER", SequenceNumber: 0})

		if err != nil {
			log.Fatal(err)
		}

		if reply.Status != pb.OK {
			log.Fatal("Not leader!")
		}

		go func(clientID uint32) {
			wg.Done()
			wg.Wait()

			for {
				go sendCommand(clientID)

				select {
				case <-time.After(wait):
				case <-stop:
					return
				}
			}
		}(reply.ClientID)
	}

	wg.Wait()

	<-reset

	time.AfterFunc(t, func() {
		log.Println("Throughput over:", t)
		log.Println(count, float64(count)/(t.Seconds()))

		close(stop)
	})

	<-stop
	log.Println("Waiting:", t/2)
	<-time.After(t / 2)
}

func sendCommand(clientID uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reply, err := client.ClientCommand(ctx, &pb.ClientCommandRequest{Command: "xxxxxxxxxxxxxxxx", SequenceNumber: <-seq, ClientID: clientID})

	if err == nil && reply.Status == pb.OK {
		counter <- struct{}{}
	}
}
