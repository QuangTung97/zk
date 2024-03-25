package curator

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/zk"
)

const client1 = "client01"

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
}
