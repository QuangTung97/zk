package curator

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/zk"
)

func startCuratorClient(store *FakeZookeeper, client FakeClientID, initFn func(sess *Session)) {
	c1 := New(initFn)
	f1 := NewFakeClientFactory(store, client)
	f1.Start(c1)
}

type simpleLock struct {
}

func newSimpleLock(store *FakeZookeeper, client FakeClientID) *simpleLock {
	l := &simpleLock{}
	startCuratorClient(store, client, l.start)
	return l
}

func (l *simpleLock) start(sess *Session) {
	sess.GetClient().Create("/master", nil, zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {
		if err != nil {
			if errors.Is(err, zk.ErrNodeExists) {
				l.onFollower(sess)
				return
			}
			if errors.Is(err, zk.ErrConnectionClosed) {
				return
			}
			panic(err)
		}
		l.isLeader(sess)
	})
}

func (l *simpleLock) onFollower(sess *Session) {
	sess.GetClient().GetW("/master", func(resp zk.GetResponse, err error) {
		if err != nil {
			if errors.Is(err, zk.ErrConnectionClosed) {
				sess.AddRetry(l.onFollower)
				return
			}
			if errors.Is(err, zk.ErrNoNode) {
				l.onFollower(sess)
				return
			}
			panic(err)
		}
	}, func(ev zk.Event) {
		l.start(sess)
	})
}

func (l *simpleLock) isLeader(sess *Session) {
	sess.GetClient().Get("/counter", func(resp zk.GetResponse, err error) {
		if err != nil {
			if errors.Is(err, zk.ErrNoNode) {
				l.increase(sess, 1, 0)
				return
			}
			if errors.Is(err, zk.ErrConnectionClosed) {
				sess.AddRetry(l.isLeader)
				return
			}
			panic(err)
		}

		num, err := strconv.ParseInt(string(resp.Data), 10, 64)
		if err != nil {
			panic(err)
		}
		l.increase(sess, int(num)+1, resp.Stat.Version)
	})
}

func numToBytes(val int) []byte {
	return []byte(strconv.FormatInt(int64(val), 10))
}

func (l *simpleLock) setCounterResp(sess *Session) func(_ zk.SetResponse, err error) {
	return func(_ zk.SetResponse, err error) {
		if err != nil {
			if errors.Is(err, zk.ErrConnectionClosed) {
				sess.AddRetry(l.isLeader)
				return
			}
			panic(err)
		}
		l.isLeader(sess)
	}
}

func (l *simpleLock) createCounterResp(sess *Session) func(_ zk.CreateResponse, err error) {
	return func(_ zk.CreateResponse, err error) {
		if err != nil {
			if errors.Is(err, zk.ErrConnectionClosed) {
				sess.AddRetry(l.isLeader)
				return
			}
			panic(err)
		}
		l.isLeader(sess)
	}
}

func (l *simpleLock) increase(sess *Session, nextVal int, version int32) {
	client := sess.GetClient()
	if nextVal > 1 {
		client.Set("/counter", numToBytes(nextVal), version, l.setCounterResp(sess))
	} else {
		client.Create("/counter", numToBytes(nextVal), 0, l.createCounterResp(sess))
	}
}

func TestFakeZookeeperTester_Master_Lock(t *testing.T) {
	store := NewFakeZookeeper()
	tester := NewFakeZookeeperTester(store, []FakeClientID{
		client1,
		client2,
		client3,
	}, 1234)

	newSimpleLock(store, client1)
	newSimpleLock(store, client2)
	newSimpleLock(store, client3)

	tester.Begin()

	steps := tester.RunSessionExpiredAndConnectionError(
		10,
		10,
		1000,
	)
	assert.Equal(t, 1000, steps)

	store.PrintData()
	store.PrintPendingCalls()

	node := &ZNode{}
	for _, n := range store.Root.Children {
		if n.Name == "counter" {
			node = n
			break
		}
	}
	assert.Equal(t, "274", string(node.Data))
}

func TestFakeZookeeperTester_Master_Lock__Multi_Times(t *testing.T) {
	for i := 0; i < 1000; i++ {
		seed := time.Now().UnixNano()
		fmt.Println("SEED:", seed)

		store := NewFakeZookeeper()
		tester := NewFakeZookeeperTester(store, []FakeClientID{
			client1,
			client2,
			client3,
		}, seed)

		newSimpleLock(store, client1)
		newSimpleLock(store, client2)
		newSimpleLock(store, client3)

		tester.Begin()

		tester.RunSessionExpiredAndConnectionError(
			10,
			10,
			1000,
		)
	}
}

func TestFakeZookeeperTester_Master_Lock__Multi_Times__With_Ops_Error(t *testing.T) {
	for i := 0; i < 1000; i++ {
		seed := time.Now().UnixNano()
		fmt.Println("SEED:", seed)

		store := NewFakeZookeeper()
		tester := NewFakeZookeeperTester(store, []FakeClientID{
			client1,
			client2,
			client3,
		}, seed)

		newSimpleLock(store, client1)
		newSimpleLock(store, client2)
		newSimpleLock(store, client3)

		tester.Begin()

		tester.RunSessionExpiredAndConnectionError(
			10,
			10,
			1000,
			WithRunOperationErrorPercentage(15),
		)
	}
}

func TestRunConfig(t *testing.T) {
	c := newRunConfig(WithRunOperationErrorPercentage(20))

	source := rand.New(rand.NewSource(1234))
	trueCount := 0
	for i := 0; i < 1000; i++ {
		if c.operationShouldError(source) {
			trueCount++
		}
	}
	assert.Equal(t, 214, trueCount)
}
