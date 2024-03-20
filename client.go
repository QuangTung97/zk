package zk

import (
	"errors"
	"io"
	"sync"
	"time"
)

// Client ...
type Client struct {
	// =================================
	// protecting following fields
	// =================================
	mut sync.Mutex

	writeCodec codecBuffer
	readCodec  codecBuffer

	passwd       []byte
	recvTimeout  time.Duration
	pingInterval time.Duration

	state State

	lastZxid         int64
	sessionTimeoutMs int32
	sessionID        int64
	// =================================
}

// Option ...
type Option func(c *Client)

// NewClient ...
func NewClient(servers []string, sessionTimeout time.Duration, options ...Option) (*Client, error) {
	if len(servers) == 0 {
		return nil, errors.New("zk: server list must not be empty")
	}
	if sessionTimeout < 1*time.Second {
		return nil, errors.New("zk: session timeout must not be too small")
	}

	servers = FormatServers(servers)

	c := &Client{
		state:  StateDisconnected,
		passwd: emptyPassword,
	}

	c.setTimeouts(int32(sessionTimeout / time.Millisecond))

	return c, nil
}

type tcpConn interface {
	io.Reader
	io.Writer

	// SetReadDeadline sets the deadline for future Read calls
	// and any currently-blocked Read call.
	// A zero value for t means Read will not time out.
	SetReadDeadline(d time.Duration) error

	// SetWriteDeadline sets the deadline for future Write calls
	// and any currently-blocked Write call.
	// Even if write times out, it may return n > 0, indicating that
	// some of the data was successfully written.
	// A zero value for t means Write will not time out.
	SetWriteDeadline(d time.Duration) error
}

func (c *Client) setTimeouts(sessionTimeoutMs int32) {
	c.sessionTimeoutMs = sessionTimeoutMs
	sessionTimeout := time.Duration(sessionTimeoutMs) * time.Millisecond
	c.recvTimeout = sessionTimeout * 2 / 3
	c.pingInterval = c.recvTimeout / 2
}

func (c *Client) authenticate(conn tcpConn) error {
	req := &connectRequest{
		ProtocolVersion: protocolVersion,
		LastZxidSeen:    c.lastZxid,
		TimeOut:         c.sessionTimeoutMs,
		SessionID:       c.sessionID,
		Passwd:          c.passwd,
	}

	// Encode and send connect request
	_ = conn.SetWriteDeadline(c.recvTimeout * 10)
	_, err := encodeObject[connectRequest](req, &c.writeCodec, conn)
	_ = conn.SetWriteDeadline(0)
	if err != nil {
		return err
	}

	// Receive and decode a connect response.
	r := connectResponse{}

	_ = conn.SetReadDeadline(c.recvTimeout * 10)
	err = decodeObject[connectResponse](&r, &c.readCodec, conn)
	_ = conn.SetReadDeadline(0)
	if err != nil {
		return err
	}

	if r.SessionID == 0 {
		c.mut.Lock()

		c.sessionID = 0
		c.passwd = emptyPassword
		c.lastZxid = 0
		c.state = StateExpired

		c.mut.Unlock()

		return ErrSessionExpired
	}

	c.mut.Lock()
	c.sessionID = r.SessionID
	c.setTimeouts(r.TimeOut)
	c.passwd = r.Passwd
	c.state = StateHasSession
	c.mut.Unlock()

	return nil
}
