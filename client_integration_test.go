//go:build integration

package zk

import (
	"fmt"
	"net"
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
