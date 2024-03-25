package curator

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/zk"
)

const client1 FakeClientID = "client01"

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

func TestFakeClient_CreateThenListChildren(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		c := newFakeClientTest()

		initFn := func(sess *Session) {
			sess.Run(func(client Client) {
				client.Create(
					"/workers", []byte("data01"), zk.FlagEphemeral,
					func(resp zk.CreateResponse, err error) {
						c.addStep("create-resp")
					},
				)
				client.Children("/", func(resp zk.ChildrenResponse, err error) {
					c.addStep("children-resp")
				})
			})
		}

		// init client 1
		c1 := New(initFn)
		f1 := NewFakeClientFactory(c.store, client1)
		f1.Start(c1)

		c.store.Begin(client1)

		assert.Equal(t, []string{"create", "children"}, c.store.PendingCalls(client1))
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
