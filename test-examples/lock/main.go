package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/QuangTung97/zk/concurrency"
	"github.com/QuangTung97/zk/curator"
)

func main() {
	factory := curator.NewClientFactory([]string{"localhost"}, "user01", "password01")
	defer factory.Close()

	var data [16]byte
	_, err := rand.Reader.Read(data[:])
	if err != nil {
		panic(err)
	}

	nodeID := hex.EncodeToString(data[:])
	fmt.Println("NODEID:", nodeID)

	l := concurrency.NewLock("/workers", nodeID, func(sess *curator.Session) {
		fmt.Println("Lock Granted:", nodeID)
	})

	factory.Start(l.Curator())

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
