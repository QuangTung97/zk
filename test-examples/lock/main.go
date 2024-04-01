package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/QuangTung97/zk/concurrency"
	"github.com/QuangTung97/zk/curator"
)

func newNodeID() string {
	var data [16]byte
	_, err := rand.Reader.Read(data[:])
	if err != nil {
		panic(err)
	}

	return hex.EncodeToString(data[:])
}

func main() {
	factory := curator.NewClientFactory(
		[]string{
			"localhost:2181",
			"localhost:2182",
			"localhost:2183",
		},
		"user01", "password01",
	)
	defer factory.Close()

	nodeID := newNodeID()
	fmt.Println("NODEID:", nodeID)

	l := concurrency.NewLock("/workers", nodeID)

	onGranted := func(sess *curator.Session, next func(sess *curator.Session)) {
		log.Printf("\033[1;32m[Lock Granted]: %s\033[0m", nodeID)
	}

	factory.Start(curator.NewChain(
		l.Start,
		onGranted,
	))

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	for i := 0; i < 6000; i++ {
		time.Sleep(1 * time.Second)
		select {
		case <-ch:
			return
		default:
		}
	}
}
