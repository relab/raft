package main

import (
	"flag"
	"log"
	"sync"
	"time"

	"github.com/relab/raft/proto/gorums"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var client gorums.RaftClient

var counter chan interface{}
var seq chan uint64

var leader = flag.String("leader", "", "leader server address")

func main() {
	flag.Parse()

	if *leader == "" {
		log.Fatal("Leader server address not specified.")
	}

	counter = make(chan interface{})
	seq = make(chan uint64)
	stop := make(chan interface{})

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

	client = mgr.Nodes(false)[0].RaftClient

	count := 0

	go func() {
		for {
			select {
			case <-counter:
				count++
			case <-stop:
				return
			}
		}
	}()

	n := 1

	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		reply, err := client.ClientCommand(ctx, &gorums.ClientCommandRequest{Command: "REGISTER", SequenceNumber: 0})

		if err != nil {
			log.Fatal(err)
		}

		if reply.Status != gorums.OK {
			log.Fatal("Not leader!")
		}

		go func(clientID uint32) {
			wg.Done()
			wg.Wait()
			for {
				go sendCommand(clientID)
				<-time.After(50 * time.Microsecond)
			}
		}(reply.ClientID)
	}

	t := 5 * time.Second

	wg.Wait()
	time.AfterFunc(t, func() {
		close(stop)
	})

	<-stop
	<-time.After(50 * time.Millisecond)

	select {
	case <-counter:
	default:
	}

	log.Println(count, float64(count)/(t.Seconds()))
}

func sendCommand(clientID uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	reply, err := client.ClientCommand(ctx, &gorums.ClientCommandRequest{Command: "xxxxxxxxxxxxxxxx", SequenceNumber: <-seq, ClientID: clientID})

	if err == nil && reply.Status == gorums.OK {
		counter <- struct{}{}
	}
}
