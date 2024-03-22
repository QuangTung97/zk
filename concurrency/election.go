package concurrency

import (
	"errors"
	"strings"

	"github.com/QuangTung97/zk"
)

type Election struct {
	nodeID string
	parent string

	client   *zk.Client
	onLeader func(sess *LeaderSession)

	sess *LeaderSession

	reconnectCallbacks []func()
}

func NewElection(
	nodeID string,
	parent string,
	onLeader func(sess *LeaderSession),
) *Election {
	return &Election{
		nodeID:   nodeID,
		parent:   parent,
		onLeader: onLeader,
	}
}

func (e *Election) Reconnect() {
	for _, callback := range e.reconnectCallbacks {
		callback()
	}
	e.reconnectCallbacks = nil
}

func (e *Election) addRetry(callback func()) {
	e.reconnectCallbacks = append(e.reconnectCallbacks, callback)
}

func (e *Election) SessionEstablished(c *zk.Client) {
	e.client = c
	e.reconnectCallbacks = nil
	e.sess = &LeaderSession{
		election: e,
	}

	e.firstListChildren()
}

func (e *Election) firstListChildren() {
	e.client.Children(e.parent, func(resp zk.ChildrenResponse, err error) {
		if errors.Is(err, zk.ErrConnectionClosed) {
			e.addRetry(e.firstListChildren)
			return
		}
		if err != nil {
			panic(err)
		}
		status := e.checkElectionStatus(resp)
		if status == electionStatusIsLeader {
			e.onLeader(e.sess)
			return
		}
		if status == electionStatusWithoutEphemeralNode {
			e.createEphemeralNode()
			return
		}
	})
}

type electionStatus int

const (
	electionStatusWithoutEphemeralNode electionStatus = iota
	electionStatusIsFollower
	electionStatusIsLeader
)

func (e *Election) checkElectionStatus(resp zk.ChildrenResponse) electionStatus {
	status := electionStatusWithoutEphemeralNode

	minSeq := "9999999999"
	minID := ""

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
		}

		if id == e.nodeID {
			status = electionStatusIsFollower
		}
	}

	if minID == e.nodeID {
		return electionStatusIsLeader
	}

	return status
}

func (e *Election) createEphemeralNode() {
	p := e.parent + "/node:" + e.nodeID + "-"
	e.client.Create(
		p, nil, zk.FlagEphemeral|zk.FlagSequence,
		zk.WorldACL(zk.PermAll), // TODO config permission
		func(resp zk.CreateResponse, err error) {
			if errors.Is(err, zk.ErrConnectionClosed) {
				e.addRetry(e.firstListChildren)
				return
			}
			if err != nil {
				panic(err)
			}
		},
	)
}

type LeaderSession struct {
	election *Election
}

func (s *LeaderSession) Run(fn func(c *zk.Client)) {
	sess := s.election.sess
	if sess != s {
		return
	}
	fn(s.election.client)
}
