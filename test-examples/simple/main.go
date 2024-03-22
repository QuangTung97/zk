package main

import (
	"fmt"
	"time"

	"github.com/go-zookeeper/zk"
)

func main() {
	c, err := zk.NewClient([]string{"localhost"}, 12*time.Second,
		zk.WithReconnectingCallback(func(c *zk.Client) {
			fmt.Println("RECONNECTING")
		}),
		zk.WithSessionEstablishedCallback(func(c *zk.Client) {
			fmt.Println("SESSION ESTABLISHED")
			c.Create("/workers-data", []byte("data01"),
				zk.FlagEphemeral, zk.WorldACL(zk.PermAll),
				func(resp zk.CreateResponse, err error) {
					fmt.Println("CREATED SUCCESSFUL:", resp, err)
				},
			)
		}),
		zk.WithSessionExpiredCallback(func(c *zk.Client) {
			fmt.Println("SESSION EXPIRED")
		}),
	)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 30; i++ {
		time.Sleep(1 * time.Second)
		fmt.Println("SLEPT:", i+1)
	}

	defer c.Close()
}
