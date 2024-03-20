package zk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
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

func TestClient_NextXID(t *testing.T) {
	c := &Client{nextXidValue: 12}
	assert.Equal(t, int32(13), c.nextXid())

	// wrap around
	c = &Client{nextXidValue: math.MaxInt32}
	assert.Equal(t, int32(0), c.nextXid())

	c = &Client{nextXidValue: math.MaxInt32 - 1}
	assert.Equal(t, int32(math.MaxInt32), c.nextXid())
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

type clientTest struct {
	client *Client
	conn   *connMock
	codec  codecBuffer
}

func newClientTest(_ *testing.T) *clientTest {
	c, err := NewClient([]string{"server01"}, 6*time.Second)
	if err != nil {
		panic(err)
	}
	return &clientTest{
		client: c,
		conn:   &connMock{},
	}
}

func (c *clientTest) doAuthenticate() {
	var err error

	resp := connectResponse{
		TimeOut:   12000, // recv time = 8 seconds
		SessionID: 3400,
		Passwd:    []byte("new-pass"),
	}
	_, err = encodeObject[connectResponse](&resp, &c.codec, &c.conn.readBuf)
	if err != nil {
		panic(err)
	}

	err = c.client.authenticate(c.conn)
	if err != nil {
		panic(err)
	}

	c.conn.writeBuf.Reset()
	c.conn.readBuf.Reset()
	c.conn.readDuration = nil
	c.conn.writeDuration = nil
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
		c := newClientTest(t)

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
		c := newClientTest(t)

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
		c := newClientTest(t)
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

func TestClient_SendData(t *testing.T) {
	t.Run("check request data", func(t *testing.T) {
		c := newClientTest(t)
		c.doAuthenticate()

		req := &CreateRequest{
			Path:  "/workers",
			Data:  []byte("data 01"),
			Acl:   WorldACL(PermAll),
			Flags: FlagEphemeral,
		}

		_ = c.client.sendData(c.conn, clientRequest{
			xid:     21,
			opcode:  opCreate,
			request: req,
		})

		buf := c.conn.writeBuf.Bytes()

		assert.Equal(t, []byte{
			0, 0, 0, 0x3e,
		}, buf[:4])

		var header requestHeader
		n, err := decodePacket(buf[4:], &header)
		assert.Equal(t, nil, err)
		assert.Equal(t, 8, n)
		assert.Equal(t, requestHeader{
			Xid:    21,
			Opcode: opCreate,
		}, header)

		var cmpReq CreateRequest
		_, err = decodePacket(buf[4+n:], &cmpReq)
		assert.Equal(t, nil, err)
		assert.Equal(t, *req, cmpReq)

		assert.Equal(t, []time.Duration{
			8 * time.Second,
			0,
		}, c.conn.writeDuration)
	})
}

func TestClient_RecvData(t *testing.T) {
	const xid1 = 21

	t.Run("normal", func(t *testing.T) {
		c := newClientTest(t)
		c.doAuthenticate()

		req := &CreateRequest{
			Path:  "/workers",
			Data:  []byte("data 01"),
			Acl:   WorldACL(PermAll),
			Flags: FlagEphemeral,
		}

		_ = c.client.sendData(c.conn, clientRequest{
			xid:      xid1,
			opcode:   opCreate,
			request:  req,
			response: &createResponse{},
		})

		buf := make([]byte, 2048)
		n1, _ := encodePacket(buf[4:], &responseHeader{
			Xid:  xid1,
			Zxid: 71,
			Err:  0,
		})
		n2, _ := encodePacket(buf[4+n1:], &createResponse{
			Path: "/workers-resp",
		})
		binary.BigEndian.PutUint32(buf[:4], uint32(n1+n2))

		c.conn.readBuf.Write(buf[:4+n1+n2])

		err := c.client.readSingleData(c.conn)
		assert.Equal(t, nil, err)

		// Check Handle Queue
		assert.Equal(t, 1, len(c.client.handleQueue))
		assert.Equal(t, handleEvent{
			state: StateHasSession,
			req: clientRequest{
				xid:     xid1,
				opcode:  opCreate,
				request: req,
				response: &createResponse{
					Path: "/workers-resp",
				},
			},
		}, c.client.handleQueue[0])
	})
}
