//go:build integration

package zk

import (
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type tcpConnTest struct {
	conn net.Conn
}

func (c *tcpConnTest) Read(p []byte) (n int, err error) {
	return c.conn.Read(p)
}

func (c *tcpConnTest) Write(p []byte) (n int, err error) {
	return c.conn.Write(p)
}

func (c *tcpConnTest) SetReadDeadline(d time.Duration) error {
	return c.conn.SetReadDeadline(time.Now().Add(d))
}

func (c *tcpConnTest) SetWriteDeadline(d time.Duration) error {
	return c.conn.SetWriteDeadline(time.Now().Add(d))
}

func (c *tcpConnTest) Close() error {
	return c.conn.Close()
}

func TestClientIntegration_Authenticate(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		c, err := newClientInternal([]string{"localhost"}, 12*time.Second)
		assert.Equal(t, nil, err)

		netConn, err := net.Dial("tcp", "localhost:2181")
		assert.Equal(t, nil, err)
		defer func() { _ = netConn.Close() }()

		err = c.authenticate(&tcpConnTest{
			conn: netConn,
		})
		assert.Equal(t, nil, err)

		fmt.Println("SESSION_ID:", c.sessionID)
		fmt.Println("PASS:", c.passwd)
		assert.Equal(t, int32(12_000), c.sessionTimeoutMs)
	})
}

func (c *tcpConnTest) closeSession(client *Client) {
	client.enqueueRequest(
		opClose,
		&closeRequest{},
		&closeResponse{},
		nil,
	)

	reqs, _ := client.getFromSendQueue()
	err := client.sendData(c, reqs[0])
	if err != nil {
		panic(err)
	}

	client.readSingleData(c)
}

func TestClientIntegration_Authenticate_And_Create(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		c, err := newClientInternal([]string{"localhost"}, 12*time.Second)
		assert.Equal(t, nil, err)

		netConn, err := net.Dial("tcp", "localhost:2181")
		assert.Equal(t, nil, err)
		defer func() { _ = netConn.Close() }()

		connTest := &tcpConnTest{
			conn: netConn,
		}
		defer connTest.closeSession(c)

		// do authenticate
		err = c.authenticate(connTest)
		assert.Equal(t, nil, err)

		req := &CreateRequest{
			Path:  "/workers",
			Data:  []byte("data01"),
			Acl:   WorldACL(PermAll),
			Flags: FlagEphemeral,
		}

		c.enqueueRequest(
			opCreate,
			req,
			&createResponse{},
			nil,
		)

		reqs, ok := c.getFromSendQueue()
		assert.Equal(t, true, ok)
		assert.Equal(t, 1, len(reqs))

		// Do Send
		err = c.sendData(connTest, reqs[0])
		assert.Equal(t, nil, err)

		// Recv Response
		c.readSingleData(connTest)
		assert.Equal(t, 1, len(c.handleQueue))

		c.handleQueue[0].zxid = 0
		assert.Equal(t, handleEvent{
			state: StateHasSession,
			req: clientRequest{
				xid:    1,
				opcode: opCreate,
				request: &CreateRequest{
					Path:  "/workers",
					Data:  []byte("data01"),
					Acl:   WorldACL(PermAll),
					Flags: 1,
				},
				response: &createResponse{
					Path: "/workers",
				},
			},
		}, c.handleQueue[0])
	})
}

func mustNewClient(_ *testing.T) *Client {
	ch := make(chan struct{})
	c, err := NewClient(
		[]string{"localhost"}, 30*time.Second,
		WithSessionEstablishedCallback(func() {
			close(ch)
		}),
	)
	if err != nil {
		panic(err)
	}
	<-ch
	return c
}

func TestClient_Create_And_Get(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		c := mustNewClient(t)

		var createResp CreateResponse
		var createErr error
		var calls int

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				calls++
				createResp = resp
				createErr = err
			},
		)
		c.Close()

		assert.Equal(t, nil, createErr)
		assert.Equal(t, 1, calls)

		assert.Greater(t, createResp.Zxid, int64(0))
		createResp.Zxid = 0
		assert.Equal(t, CreateResponse{
			Path: "/workers01",
		}, createResp)
	})

	t.Run("get children", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			nil,
		)

		c.Create(
			"/workers02", []byte("data02"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-resp-02")
			},
		)

		c.Create(
			"/workers03", []byte("data03"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-resp-03")
			},
		)

		var childrenResp ChildrenResponse
		var childrenErr error

		c.Children("/", func(resp ChildrenResponse, err error) {
			steps = append(steps, "children")
			childrenResp = resp
			childrenErr = err
		})

		c.Close()

		assert.Equal(t, []string{
			"create-resp-02",
			"create-resp-03",
			"children",
		}, steps)
		assert.Equal(t, nil, childrenErr)

		assert.Greater(t, childrenResp.Zxid, int64(0))
		childrenResp.Zxid = 0

		slices.Sort(childrenResp.Children)
		assert.Equal(t, ChildrenResponse{
			Children: []string{
				"workers01",
				"workers02",
				"workers03",
				"zookeeper",
			},
		}, childrenResp)
	})
}
