package zk

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewClient_Validate(t *testing.T) {
	t.Run("empty servers", func(t *testing.T) {
		client, err := NewClient(nil, 0)
		assert.Equal(t, errors.New("zk: server list must not be empty"), err)
		assert.Nil(t, client)
	})

	t.Run("timeout too small", func(t *testing.T) {
		client, err := NewClient([]string{"localhost"}, 0)
		assert.Equal(t, errors.New("zk: session timeout must not be too small"), err)
		assert.Nil(t, client)
	})
}

type connMock struct {
	writeBuf bytes.Buffer
	readBuf  bytes.Buffer

	readDuration  []time.Duration
	writeDuration []time.Duration
}

func (c *connMock) Write(d []byte) (int, error) {
	return c.writeBuf.Write(d)
}

func (c *connMock) Read(d []byte) (int, error) {
	return c.readBuf.Read(d)
}

func (c *connMock) SetReadDeadline(d time.Duration) error {
	c.readDuration = append(c.readDuration, d)
	return nil
}

func (c *connMock) SetWriteDeadline(d time.Duration) error {
	c.writeDuration = append(c.writeDuration, d)
	return nil
}

type clientAuthTest struct {
	client *Client
	conn   *connMock
	codec  codecBuffer
}

func newClientAuthTest(_ *testing.T) *clientAuthTest {
	c, err := NewClient([]string{"server01"}, 6*time.Second)
	if err != nil {
		panic(err)
	}
	return &clientAuthTest{
		client: c,
		conn:   &connMock{},
	}
}

func TestClient_Authenticate(t *testing.T) {
	t.Run("check init state", func(t *testing.T) {
		c, err := NewClient([]string{"server01"}, 6*time.Second)
		assert.Equal(t, nil, err)
		assert.Equal(t, StateDisconnected, c.state)
		assert.Equal(t, []byte{
			0, 0, 0, 0,
			0, 0, 0, 0,
			0, 0, 0, 0,
			0, 0, 0, 0,
		}, c.passwd)
		assert.Equal(t, 4*time.Second, c.recvTimeout)
		assert.Equal(t, 2*time.Second, c.pingInterval)
		assert.Equal(t, int64(0), c.lastZxid)
		assert.Equal(t, int32(6000), c.sessionTimeoutMs)
		assert.Equal(t, int64(0), c.sessionID)
	})

	t.Run("check request data", func(t *testing.T) {
		c := newClientAuthTest(t)

		var err error
		err = c.client.authenticate(c.conn)
		assert.Equal(t, io.EOF, err)

		var req connectRequest
		err = decodeObject[connectRequest](&req, &c.codec, &c.conn.writeBuf)
		assert.Equal(t, nil, err)
		assert.Equal(t, connectRequest{
			ProtocolVersion: 0,
			LastZxidSeen:    0,
			TimeOut:         6000,
			SessionID:       0,
			Passwd:          emptyPassword,
		}, req)

		assert.Equal(t, []time.Duration{
			40 * time.Second,
			0,
		}, c.conn.writeDuration)
	})

	t.Run("handle response", func(t *testing.T) {
		c := newClientAuthTest(t)

		var err error

		resp := connectResponse{
			TimeOut:   12000,
			SessionID: 3400,
			Passwd:    []byte("new-pass"),
		}
		_, err = encodeObject[connectResponse](&resp, &c.codec, &c.conn.readBuf)
		assert.Equal(t, nil, err)

		err = c.client.authenticate(c.conn)
		assert.Equal(t, nil, err)

		assert.Equal(t, []time.Duration{
			40 * time.Second,
			0,
		}, c.conn.readDuration)

		// check state
		assert.Equal(t, StateHasSession, c.client.state)
		assert.Equal(t, []byte("new-pass"), c.client.passwd)
		assert.Equal(t, 8*time.Second, c.client.recvTimeout)
		assert.Equal(t, 4*time.Second, c.client.pingInterval)
		assert.Equal(t, int64(0), c.client.lastZxid)
		assert.Equal(t, int32(12000), c.client.sessionTimeoutMs)
		assert.Equal(t, int64(3400), c.client.sessionID)
	})

	t.Run("handle session expired", func(t *testing.T) {
		c := newClientAuthTest(t)
		c.client.sessionID = 3400
		c.client.passwd = []byte("some-pass")
		c.client.lastZxid = 8020
		c.client.state = StateDisconnected

		var err error

		resp := connectResponse{
			TimeOut: 12000,
		}
		_, err = encodeObject[connectResponse](&resp, &c.codec, &c.conn.readBuf)
		assert.Equal(t, nil, err)

		err = c.client.authenticate(c.conn)
		assert.Equal(t, ErrSessionExpired, err)

		assert.Equal(t, []time.Duration{
			40 * time.Second,
			0,
		}, c.conn.readDuration)

		// check state
		assert.Equal(t, StateExpired, c.client.state)
		assert.Equal(t, emptyPassword, c.client.passwd)
		assert.Equal(t, 4*time.Second, c.client.recvTimeout)
		assert.Equal(t, 2*time.Second, c.client.pingInterval)
		assert.Equal(t, int64(0), c.client.lastZxid)
		assert.Equal(t, int32(6000), c.client.sessionTimeoutMs)
		assert.Equal(t, int64(0), c.client.sessionID)
	})
}
