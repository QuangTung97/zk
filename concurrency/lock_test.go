package concurrency

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
)

const client1 curator.FakeClientID = "client1"
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

func TestLock(t *testing.T) {
	e := NewLock("/workers", "node01")

	store := initStore("/workers")

	c := curator.NewFakeClientFactory(store, client1)
	c.Start(e.Curator())

	store.Begin(client1)
	assert.Equal(t, []string{"children"}, store.PendingCalls(client1))

	store.ChildrenApply(client1)
	assert.Equal(t, []string{"create"}, store.PendingCalls(client1))

	store.CreateApply(client1)
	assert.Equal(t, []string{"children"}, store.PendingCalls(client1))

	store.ChildrenApply(client1)
	assert.Equal(t, []string{}, store.PendingCalls(client1))
}
