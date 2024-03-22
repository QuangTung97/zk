package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
)

type Election struct {
	parent string
	nodeID string
}

type electionStatus int

const (
	electionStatusWithoutEphemeralNode electionStatus = iota
	electionStatusIsFollower
	electionStatusIsLeader
)

func (e *Election) checkElectionStatus(resp zk.ChildrenResponse) (electionStatus, string) {
	status := electionStatusWithoutEphemeralNode

	minSeq := "9999999999"
	minID := ""
	prevZnode := "" // TODO

	for _, child := range resp.Children {
		parts := strings.Split(child, "-")
		if len(parts) != 2 {
			continue
		}
		first := parts[0]
		sequence := parts[1]

		nodeNameParts := strings.Split(first, ":")
		if len(nodeNameParts) != 2 {
			continue
		}

		id := nodeNameParts[1]
		if sequence < minSeq {
			minSeq = sequence
			minID = id
			prevZnode = child
		}

		if id == e.nodeID {
			status = electionStatusIsFollower
		}
	}

	if minID == e.nodeID {
		return electionStatusIsLeader, ""
	}

	return status, prevZnode
}

func (e *Election) createEphemeralNode(sess *curator.Session) {
	sess.Run(func(client *zk.Client) {
		p := e.parent + "/node:" + e.nodeID + "-"
		client.Create(
			p, nil,
			zk.FlagEphemeral|zk.FlagSequence,
			zk.WorldACL(zk.PermAll),
			func(resp zk.CreateResponse, err error) {
				if errors.Is(err, zk.ErrConnectionClosed) {
					sess.AddRetry(e.initFunc)
					return
				}
				if err != nil {
					panic(err)
				}
				e.initFunc(sess)
			},
		)
	})
}

func (e *Election) waitForPrevNode(sess *curator.Session, prevZnode string) {
	sess.Run(func(client *zk.Client) {
		client.Get(prevZnode, func(resp zk.GetResponse, err error) {
			if errors.Is(err, zk.ErrConnectionClosed) {
				sess.AddRetry(e.initFunc)
				return
			}
			if errors.Is(err, zk.ErrNoNode) {
				e.initFunc(sess)
				return
			}
		}, zk.WithGetWatch(func(ev zk.Event) {
			if ev.Type == zk.EventNodeDeleted {
				e.initFunc(sess)
				return
			}
		}))
	})
}

func (e *Election) initFunc(sess *curator.Session) {
	sess.Run(func(client *zk.Client) {
		client.Children(e.parent, func(resp zk.ChildrenResponse, err error) {
			if errors.Is(err, zk.ErrConnectionClosed) {
				sess.AddRetry(e.initFunc)
				return
			}
			if err != nil {
				panic(err)
			}

			status, prevZnode := e.checkElectionStatus(resp)
			if status == electionStatusWithoutEphemeralNode {
				e.createEphemeralNode(sess)
				return
			}
			if status == electionStatusIsFollower {
				e.waitForPrevNode(sess, prevZnode)
				return
			}
			fmt.Println("LEADER BEGIN")
		})
	})
}

func main() {
	election := &Election{}
	leaderCurator := curator.New(
		election.initFunc,
	)

	c, err := zk.NewClient(
		[]string{"localhost"}, 12*time.Second,
		zk.WithSessionEstablishedCallback(func(c *zk.Client) {
			leaderCurator.Begin(c)
		}),
		zk.WithReconnectingCallback(func(c *zk.Client) {
			leaderCurator.Retry()
		}),
		zk.WithSessionExpiredCallback(func(c *zk.Client) {
			leaderCurator.End()
		}),
	)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	for i := 0; i < 600; i++ {
		time.Sleep(1 * time.Second)
		select {
		case <-ch:
			return
		default:
		}
		fmt.Println("SLEEP:", i+1)
	}
}
