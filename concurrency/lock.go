package concurrency

import (
	"errors"
	"slices"
	"strings"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
)

type Lock struct {
	parent    string
	nodeID    string
	onGranted func(sess *curator.Session)

	cur *curator.Curator
}

func NewLock(parent string, nodeID string, onGranted func(sess *curator.Session)) *Lock {
	e := &Lock{
		nodeID:    nodeID,
		parent:    parent,
		onGranted: onGranted,
	}
	e.cur = curator.New(e.initFunc)
	return e
}

type lockStatus int

const (
	lockStatusBlocked lockStatus = iota + 1
	lockStatusNeedCreate
	lockStatusGranted
)

func (e *Lock) initFunc(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.Children(e.parent, func(resp zk.ChildrenResponse, err error) {
			if err != nil {
				panic(err)
			}

			var prevNode string
			status := e.computeLockStatus(resp, &prevNode)
			if status == lockStatusNeedCreate {
				e.createEphemeral(sess)
				return
			}
			if status == lockStatusBlocked {
				e.watchPreviousNode(sess, prevNode)
				return
			}
			e.onGranted(sess)
		})
	})
}

func (e *Lock) computeLockStatus(resp zk.ChildrenResponse, prevNode *string) lockStatus {
	type nodeName struct {
		raw    string
		nodeID string
		seq    string
	}

	nodes := make([]nodeName, 0, len(resp.Children))
	for _, child := range resp.Children {
		parts := strings.Split(child, "-")
		if len(parts) < 2 {
			continue
		}

		seq := parts[1]

		parts = strings.Split(parts[0], ":")
		if len(parts) < 2 {
			continue
		}

		nodes = append(nodes, nodeName{
			raw:    child,
			nodeID: parts[1],
			seq:    seq,
		})
	}
	slices.SortFunc(nodes, func(a, b nodeName) int {
		return stringCmp(a.seq, b.seq)
	})

	if len(nodes) == 0 {
		return lockStatusNeedCreate
	}

	if nodes[0].nodeID == e.nodeID {
		return lockStatusGranted
	}

	for i, n := range nodes {
		if n.nodeID == e.nodeID {
			*prevNode = e.parent + "/" + nodes[i-1].raw
			return lockStatusBlocked
		}
	}

	return lockStatusNeedCreate
}

func stringCmp(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
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

func (e *Lock) watchPreviousNode(sess *curator.Session, prevNode string) {
	sess.Run(func(client curator.Client) {
		client.GetW(prevNode, func(resp zk.GetResponse, err error) {
			if err == nil {
				return
			}
			if errors.Is(err, zk.ErrNoNode) {
				e.initFunc(sess)
				return
			}
			panic(err)
		}, func(ev zk.Event) {
			if ev.Type == zk.EventNodeDeleted {
				e.initFunc(sess)
				return
			}
		})
	})
}

func (e *Lock) Curator() *curator.Curator {
	return e.cur

}
