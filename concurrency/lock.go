package concurrency

import (
	"fmt"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
)

type Lock struct {
	parent string
	nodeID string
	cur    *curator.Curator
}

func NewLock(parent string, nodeID string) *Lock {
	e := &Lock{
		nodeID: nodeID,
		parent: parent,
	}
	e.cur = curator.New(e.initFunc)
	return e
}

func (e *Lock) initFunc(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.Children(e.parent, func(resp zk.ChildrenResponse, err error) {
			fmt.Println(resp)
			if err != nil {
				panic(err)
			}
			e.createEphemeral(sess)
		})
	})
}

func (e *Lock) createEphemeral(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		p := e.parent + "/node:" + e.nodeID + "-"
		client.Create(p, nil, zk.FlagEphemeral|zk.FlagSequence,
			func(resp zk.CreateResponse, err error) {
				if err != nil {
					panic(err)
				}
				e.initFunc(sess)
			},
		)
	})
}

func (e *Lock) Curator() *curator.Curator {
	return e.cur

}
