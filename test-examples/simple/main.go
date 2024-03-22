package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/QuangTung97/zk"
)

func main() {
	count := 0
	c, err := zk.NewClient([]string{"localhost"}, 12*time.Second,
		zk.WithReconnectingCallback(func(c *zk.Client) {
			fmt.Println("RECONNECTING")
		}),
		zk.WithSessionEstablishedCallback(func(c *zk.Client) {
			count++
			fmt.Println("SESSION ESTABLISHED")

			if count > 1 {
				return
			}

			c.Create("/workers-data", []byte("data01"),
				zk.FlagEphemeral, zk.WorldACL(zk.PermAll),
				func(resp zk.CreateResponse, err error) {
					fmt.Println("CREATED SUCCESSFUL:", resp, err)
				},
			)

			c.Get("/sample", func(resp zk.GetResponse, err error) {
				fmt.Println("GET RESP:", resp, err)
			}, zk.WithGetWatch(func(ev zk.Event) {
				fmt.Println("GET WATCH RESP:", ev)
			}))
		}),
		zk.WithSessionExpiredCallback(func(c *zk.Client) {
			fmt.Println("SESSION EXPIRED")
		}),
	)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	for i := 0; i < 60; i++ {
		time.Sleep(1 * time.Second)
		select {
		case <-ch:
			return
		default:
		}
		fmt.Println("SLEPT:", i+1)
	}
}
