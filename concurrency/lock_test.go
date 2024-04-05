package concurrency

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
)

const client1 curator.FakeClientID = "client1"
const client2 curator.FakeClientID = "client2"
const client3 curator.FakeClientID = "client3"
const initClient curator.FakeClientID = "init"

func initStore(parent string) *curator.FakeZookeeper {
	store := curator.NewFakeZookeeper()

	c := curator.NewFakeClientFactory(store, initClient)
	c.Start(curator.New(func(sess *curator.Session) {
		sess.Run(func(client curator.Client) {
			client.Create(parent, nil, 0, func(resp zk.CreateResponse, err error) {
				if err != nil {
					panic(err)
				}
			})
		})
	}))

	store.Begin(initClient)
	store.CreateApply(initClient)
	if len(store.PendingCalls(initClient)) > 0 {
		panic("must be empty")
	}

	return store
}

func TestLock_Single_Client__Success(t *testing.T) {
	steps := make([]string, 0)
	l := NewLock("/workers", "node01")

	store := initStore("/workers")

	onGranted := func(sess *curator.Session) {
		steps = append(steps, "lock-granted")
	}
	startLock(l, store, client1, onGranted)

	store.Begin(client1)
	assert.Equal(t, []string{"children"}, store.PendingCalls(client1))

	store.ChildrenApply(client1)
	assert.Equal(t, []string{"create"}, store.PendingCalls(client1))

	store.CreateApply(client1)
	assert.Equal(t, []string{"children"}, store.PendingCalls(client1))

	assert.Equal(t, []string{}, steps)

	store.ChildrenApply(client1)
	assert.Equal(t, []string{}, store.PendingCalls(client1))

	assert.Equal(t, []string{
		"lock-granted",
	}, steps)
}

func startLock(
	l *Lock, store *curator.FakeZookeeper,
	client curator.FakeClientID, onGranted func(sess *curator.Session),
) {
	c := curator.NewFakeClientFactory(store, client)
	c.Start(curator.NewChain(
		l.Start,
		func(sess *curator.Session, next func(sess *curator.Session)) {
			onGranted(sess)
		},
	))
}

func TestLock_Two_Clients__Concurrent(t *testing.T) {
	steps := make([]string, 0)
	l1 := NewLock("/workers", "node01")
	l2 := NewLock("/workers", "node02")

	store := initStore("/workers")

	startLock(l1, store, client1, func(sess *curator.Session) {
		steps = append(steps, "lock01-granted")
	})
	startLock(l2, store, client2, func(sess *curator.Session) {
		steps = append(steps, "lock02-granted")
	})

	store.Begin(client1)
	store.Begin(client2)

	store.ChildrenApply(client1)
	store.ChildrenApply(client2)

	store.CreateApply(client1)
	store.CreateApply(client2)

	store.ChildrenApply(client1)
	store.ChildrenApply(client2)

	assert.Equal(t, []string{
		"lock01-granted",
	}, steps)

	assert.Equal(t, []string{}, store.PendingCalls(client1))
	assert.Equal(t, []string{"get-w"}, store.PendingCalls(client2))

	store.GetApply(client2)

	// Lock02 Start Granted
	store.SessionExpired(client1)

	assert.Equal(t, []string{"children"}, store.PendingCalls(client2))

	assert.Equal(t, []string{
		"lock01-granted",
	}, steps)
	store.ChildrenApply(client2)
	assert.Equal(t, []string{
		"lock01-granted",
		"lock02-granted",
	}, steps)
}

func TestLock_Two_Clients__First_Granted__Then_Second_Start(t *testing.T) {
	l1 := NewLock("/workers", "node01")
	l2 := NewLock("/workers", "node02")

	store := initStore("/workers")

	steps := make([]string, 0)

	startLock(l1, store, client1, func(sess *curator.Session) {
		steps = append(steps, "lock01-granted")
	})
	startLock(l2, store, client2, func(sess *curator.Session) {
		steps = append(steps, "lock02-granted")
	})

	store.Begin(client1)
	store.Begin(client2)

	store.ChildrenApply(client1)
	store.CreateApply(client1)
	store.ChildrenApply(client1)

	assert.Equal(t, []string{
		"lock01-granted",
	}, steps)

	assert.Equal(t, []string{}, store.PendingCalls(client1))

	store.ChildrenApply(client2)
	store.CreateApply(client2)
	store.ChildrenApply(client2)
	store.GetApply(client2)

	assert.Equal(t, []string{}, store.PendingCalls(client2))

	assert.Equal(t, []string{
		"lock01-granted",
	}, steps)
}

func TestLock_Two_Clients__First_Granted__Then_Expired_Right_Before_Client2_GetData(t *testing.T) {
	l1 := NewLock("/workers", "node01")
	l2 := NewLock("/workers", "node02")

	store := initStore("/workers")

	steps := make([]string, 0)
	startLock(l1, store, client1, func(sess *curator.Session) {
		steps = append(steps, "lock01-granted")
	})
	startLock(l2, store, client2, func(sess *curator.Session) {
		steps = append(steps, "lock02-granted")
	})

	store.Begin(client1)
	store.Begin(client2)

	store.ChildrenApply(client1)
	store.CreateApply(client1)
	store.ChildrenApply(client1)

	assert.Equal(t, []string{
		"lock01-granted",
	}, steps)

	assert.Equal(t, []string{}, store.PendingCalls(client1))

	store.ChildrenApply(client2)
	store.CreateApply(client2)
	store.ChildrenApply(client2)

	store.SessionExpired(client1)

	store.GetApply(client2)
	store.ChildrenApply(client2)

	assert.Equal(t, []string{}, store.PendingCalls(client2))

	assert.Equal(t, []string{
		"lock01-granted",
		"lock02-granted",
	}, steps)
}

func TestLock_Single_Client__Children_Error(t *testing.T) {
	steps := make([]string, 0)
	l := NewLock("/workers", "node01")

	store := initStore("/workers")

	startLock(l, store, client1, func(sess *curator.Session) {
		steps = append(steps, "lock-granted")
	})

	store.Begin(client1)
	assert.Equal(t, []string{"children"}, store.PendingCalls(client1))

	store.ConnError(client1)
	assert.Equal(t, []string{"retry"}, store.PendingCalls(client1))

	store.Retry(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)
	store.ChildrenApply(client1)

	assert.Equal(t, []string{}, store.PendingCalls(client1))

	assert.Equal(t, []string{
		"lock-granted",
	}, steps)
}

func TestLock_Single_Client__Create_Conn_Error(t *testing.T) {
	steps := make([]string, 0)
	l := NewLock("/workers", "node01")

	store := initStore("/workers")

	startLock(l, store, client1, func(sess *curator.Session) {
		steps = append(steps, "lock-granted")
	})

	store.Begin(client1)
	store.ChildrenApply(client1)

	store.ConnError(client1)
	assert.Equal(t, []string{"retry"}, store.PendingCalls(client1))

	store.Retry(client1)
	assert.Equal(t, []string{"children"}, store.PendingCalls(client1))
}

func TestLock_Single_Client__Create_Apply_Conn_Error(t *testing.T) {
	steps := make([]string, 0)
	l := NewLock("/workers", "node01")

	store := initStore("/workers")

	startLock(l, store, client1, func(sess *curator.Session) {
		steps = append(steps, "lock-granted")
	})

	store.Begin(client1)
	store.ChildrenApply(client1)

	store.CreateApplyError(client1)
	assert.Equal(t, []string{"retry"}, store.PendingCalls(client1))

	store.Retry(client1)
	assert.Equal(t, []string{"children"}, store.PendingCalls(client1))

	store.ChildrenApply(client1)
	assert.Equal(t, []string{}, store.PendingCalls(client1))

	assert.Equal(t, []string{
		"lock-granted",
	}, steps)
}

func TestLock_Two_Clients__Second_Get_Watch_Error(t *testing.T) {
	steps := make([]string, 0)
	l1 := NewLock("/workers", "node01")
	l2 := NewLock("/workers", "node02")

	store := initStore("/workers")

	startLock(l1, store, client1, func(sess *curator.Session) {
		steps = append(steps, "lock01-granted")
	})
	startLock(l2, store, client2, func(sess *curator.Session) {
		steps = append(steps, "lock02-granted")
	})

	store.Begin(client1)
	store.Begin(client2)

	store.ChildrenApply(client1)
	store.CreateApply(client1)
	store.ChildrenApply(client1)

	assert.Equal(t, []string{
		"lock01-granted",
	}, steps)

	store.ChildrenApply(client2)
	store.CreateApply(client2)
	store.ChildrenApply(client2)

	assert.Equal(t, []string{"get-w"}, store.PendingCalls(client2))

	store.ConnError(client2)
	assert.Equal(t, []string{"retry"}, store.PendingCalls(client2))
	store.Retry(client2)

	assert.Equal(t, []string{"children"}, store.PendingCalls(client2))
}

func TestSortString(t *testing.T) {
	s := []string{
		"A", "B", "EE", "D", "M", "Z", "IY", "IA",
	}
	slices.SortFunc(s, stringCmp)
	assert.Equal(t, []string{
		"A", "B", "D", "EE", "IA", "IY", "M", "Z",
	}, s)
}

func TestLock_With_Tester(t *testing.T) {
	l1 := NewLock("/workers", "node01")
	l2 := NewLock("/workers", "node02")
	l3 := NewLock("/workers", "node03")

	store := initStore("/workers")

	tester := curator.NewFakeZookeeperTester(store,
		[]curator.FakeClientID{client1, client2, client3},
		123,
	)

	startLock(l1, store, client1, newSimpleCounter(client1).isLeader)
	startLock(l2, store, client2, newSimpleCounter(client2).isLeader)
	startLock(l3, store, client3, newSimpleCounter(client3).isLeader)

	tester.Begin()

	steps := tester.RunSessionExpiredAndConnectionError(
		10,
		10,
		2000,
	)
	assert.Equal(t, 2000, steps)

	store.PrintData()
}

func TestLock_With_Tester__Multi_Times(t *testing.T) {
	for k := 0; k < 1000; k++ {
		seed := time.Now().UnixNano()
		fmt.Println("SEED:", seed)

		l1 := NewLock("/workers", "node01")
		l2 := NewLock("/workers", "node02")
		l3 := NewLock("/workers", "node03")

		store := initStore("/workers")

		tester := curator.NewFakeZookeeperTester(store,
			[]curator.FakeClientID{client1, client2, client3},
			seed,
		)

		startLock(l1, store, client1, newSimpleCounter(client1).isLeader)
		startLock(l2, store, client2, newSimpleCounter(client2).isLeader)
		startLock(l3, store, client3, newSimpleCounter(client3).isLeader)

		tester.Begin()

		steps := tester.RunSessionExpiredAndConnectionError(
			20,
			20,
			2000,
		)
		assert.Equal(t, 2000, steps)
	}
}
