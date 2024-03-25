package curator

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/zk"
)

const client1 FakeClientID = "client01"

func TestFakeClient(t *testing.T) {
	t.Run("start with list children", func(t *testing.T) {
		var steps []string

		store := NewFakeZookeeper()

		// init client 1
		c1 := New(func(sess *Session) {
			steps = append(steps, "init-c1")
			sess.Run(func(client Client) {
				client.Children("/lock", func(resp zk.ChildrenResponse, err error) {
				})
			})
		})
		f1 := NewFakeClientFactory(store, client1)
		f1.Start(c1)

		// begin zookeeper client
		store.Begin(client1)

		assert.Equal(t, []string{
			"init-c1",
		}, steps)

		assert.Equal(t, []string{
			"children",
		}, store.PendingCalls(client1))

		store.PrintPendingCalls()

		call := store.ChildrenCall(client1)
		assert.Equal(t, "/lock", call.Path)
	})

	t.Run("start with list multi children", func(t *testing.T) {
		var steps []string

		store := NewFakeZookeeper()

		// init client 1
		c1 := New(func(sess *Session) {
			steps = append(steps, "init-c1")
			sess.Run(func(client Client) {
				client.Children("/lock", func(resp zk.ChildrenResponse, err error) {
				})
				client.Children("/lock2", func(resp zk.ChildrenResponse, err error) {
				})
			})
		})
		f1 := NewFakeClientFactory(store, client1)
		f1.Start(c1)

		// begin zookeeper client
		store.Begin(client1)

		assert.Equal(t, []string{
			"init-c1",
		}, steps)

		assert.Equal(t, []string{
			"children",
		}, store.PendingCalls(client1))

		store.PrintPendingCalls()

		assert.PanicsWithValue(t, "ChildrenCall has more than one call", func() {
			store.ChildrenCall(client1)
		})

		calls := store.ChildrenCalls(client1)
		assert.Equal(t, 2, len(calls))
		assert.Equal(t, "/lock2", calls[1].Path)
	})

	t.Run("panics when not calling children", func(t *testing.T) {
		store := NewFakeZookeeper()

		// init client 1
		c1 := New(func(sess *Session) {})
		f1 := NewFakeClientFactory(store, client1)
		f1.Start(c1)

		// begin zookeeper client
		store.Begin(client1)

		assert.PanicsWithValue(t, "No children call currently pending", func() {
			store.ChildrenCall(client1)
		})
	})
}

type fakeClientTest struct {
	store *FakeZookeeper
	steps []string
}

func (c *fakeClientTest) addStep(s string) {
	c.steps = append(c.steps, s)
}

func newFakeClientTest() *fakeClientTest {
	store := NewFakeZookeeper()
	return &fakeClientTest{
		store: store,
		steps: []string{},
	}
}

func TestFakeClient_CreateUntilSuccess(t *testing.T) {
	t.Run("success on first try", func(t *testing.T) {
		c := newFakeClientTest()

		var createResp zk.CreateResponse
		var createErr error

		// init client 1
		c1 := New(func(sess *Session) {
			sess.Run(func(client Client) {
				client.Create(
					"/workers", []byte("data01"), zk.FlagEphemeral,
					func(resp zk.CreateResponse, err error) {
						createResp = resp
						createErr = err
					},
				)
			})
		})
		f1 := NewFakeClientFactory(c.store, client1)
		f1.Start(c1)

		c.store.Begin(client1)

		assert.Equal(t, []string{"create"}, c.store.PendingCalls(client1))

		// Create Success
		c.store.CreateApply(client1)

		assert.Equal(t, &ZNode{
			Children: []*ZNode{
				{
					Name:  "workers",
					Data:  []byte("data01"),
					Flags: zk.FlagEphemeral,
				},
			},
		}, c.store.Root)

		assert.Equal(t, zk.CreateResponse{
			Zxid: 101,
			Path: "/workers",
		}, createResp)
		assert.Equal(t, nil, createErr)

		assert.Equal(t, []string{}, c.store.PendingCalls(client1))
	})

	t.Run("success on second try", func(t *testing.T) {
		c := newFakeClientTest()

		var initFn func(sess *Session)
		initFn = func(sess *Session) {
			sess.Run(func(client Client) {
				client.Create(
					"/workers", []byte("data01"), zk.FlagEphemeral,
					func(resp zk.CreateResponse, err error) {
						c.addStep("create-resp")
						if err != nil {
							sess.AddRetry(initFn)
						}
					},
				)
			})
		}

		// init client 1
		c1 := New(initFn)
		f1 := NewFakeClientFactory(c.store, client1)
		f1.Start(c1)

		c.store.Begin(client1)
		assert.Equal(t, []string{"create"}, c.store.PendingCalls(client1))

		c.store.CreateConnError(client1)
		assert.Equal(t, []string{"retry"}, c.store.PendingCalls(client1))

		c.store.Retry(client1)
		assert.Equal(t, []string{"create"}, c.store.PendingCalls(client1))

		c.store.CreateApply(client1)
		assert.Equal(t, []string{}, c.store.PendingCalls(client1))

		assert.Equal(t, &ZNode{
			Children: []*ZNode{
				{
					Name:  "workers",
					Data:  []byte("data01"),
					Flags: zk.FlagEphemeral,
				},
			},
		}, c.store.Root)

		assert.Equal(t, []string{
			"create-resp",
			"create-resp",
		}, c.steps)
	})
}

func TestComputePathNodes(t *testing.T) {
	assert.Equal(t, []string{
		"/", "workers",
	}, computePathNodes("/workers"))

	assert.Equal(t, []string{
		"/",
	}, computePathNodes("/"))

	assert.Equal(t, []string{
		"/", "data", "tmp01",
	}, computePathNodes("/data/tmp01"))
}
