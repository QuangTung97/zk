package zk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func NewClient(servers []string, sessionTimeout time.Duration, options ...Option) (*Client, error) {
	c, err := newClientInternal(servers, sessionTimeout, options...)
	if err != nil {
		return nil, err
	}

	c.wg.Add(3)

	go func() {
		defer c.wg.Done()
		c.runSender()
	}()

	go func() {
		defer c.wg.Done()
		c.runReceiver()
	}()

	go func() {
		defer c.wg.Done()
		c.runHandler()
	}()

	return c, nil
}

// Client ...
type Client struct {
	servers []string

	writeCodec codecBuffer // not need to lock
	readCodec  codecBuffer // not need to lock

	// =================================
	// protect following fields
	// =================================
	mut sync.Mutex

	passwd       []byte
	recvTimeout  time.Duration
	pingInterval time.Duration

	state State

	lastZxid         int64
	sessionTimeoutMs int32
	sessionID        int64

	sendQueue []clientRequest
	sendCond  *sync.Cond

	recvMap map[int32]clientRequest

	handleQueue []handleEvent
	handleCond  *sync.Cond

	shutdown bool

	conn tcpConn
	// =================================

	// not need to lock by mutex
	nextXidValue uint32

	wg sync.WaitGroup
}

// Option ...
type Option func(c *Client)

type clientRequest struct {
	xid      int32
	opcode   int32
	request  any
	response any

	callback func(res any, zxid int64, err error)
}

type handleEvent struct {
	state State
	zxid  int64
	err   error
	req   clientRequest
}

type clientWatchEvent struct {
	Type   EventType
	State  State
	Path   string // For non-session events, the path of the watched node.
	Err    error
	Server string // For connection events
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

func newClientInternal(servers []string, sessionTimeout time.Duration, options ...Option) (*Client, error) {
	if len(servers) == 0 {
		return nil, errors.New("zk: server list must not be empty")
	}
	if sessionTimeout < 1*time.Second {
		return nil, errors.New("zk: session timeout must not be too small")
	}

	servers = FormatServers(servers)

	c := &Client{
		servers: servers,

		state:  StateDisconnected,
		passwd: emptyPassword,

		recvMap: map[int32]clientRequest{},
	}

	c.sendCond = sync.NewCond(&c.mut)
	c.handleCond = sync.NewCond(&c.mut)

	c.setTimeouts(int32(sessionTimeout / time.Millisecond))

	return c, nil
}

func (c *Client) getFromSendQueue() ([]clientRequest, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	for {
		if len(c.sendQueue) > 0 {
			requests := c.sendQueue
			c.sendQueue = nil
			return requests, true
		}

		if c.shutdown {
			return nil, false
		}

		c.sendCond.Wait()
	}
}

type tcpConnImpl struct {
	conn net.Conn
}

func (c *tcpConnImpl) Write(p []byte) (int, error) {
	return c.conn.Write(p)
}

func (c *tcpConnImpl) Read(p []byte) (int, error) {
	return c.conn.Read(p)
}

func (c *tcpConnImpl) SetReadDeadline(d time.Duration) error {
	return c.conn.SetReadDeadline(time.Now().Add(d))
}

func (c *tcpConnImpl) SetWriteDeadline(d time.Duration) error {
	return c.conn.SetWriteDeadline(time.Now().Add(d))
}

func (c *Client) tryToConnect() tcpConn {
	c.mut.Lock()
	if c.state == StateHasSession {
		c.mut.Unlock()
		return c.conn
	}
	c.state = StateConnecting
	c.mut.Unlock()

	netConn, err := net.Dial("tcp", c.servers[0])
	if err != nil {
		// TODO Handle
		panic(err)
	}

	conn := &tcpConnImpl{
		conn: netConn,
	}

	c.mut.Lock()
	c.state = StateConnected
	c.conn = conn
	c.mut.Unlock()

	err = c.authenticate(conn)
	if err != nil {
		// TODO Handle error
		panic(err)
	}

	return conn
}

func (c *Client) runSender() {
	for {
		conn := c.tryToConnect()

		requests, ok := c.getFromSendQueue()
		if !ok {
			return
		}

		for _, req := range requests {
			err := c.sendData(conn, req)
			if err != nil {
				// TODO Handle error
			}
		}

	}
}

func (c *Client) runReceiver() {
	for {
		c.mut.Lock()
		conn := c.conn
		closed := c.shutdown
		c.mut.Unlock()

		if closed {
			return
		}

		c.readSingleData(conn)
	}
}

func (c *Client) getHandleEvents() ([]handleEvent, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	for {
		if len(c.handleQueue) > 0 {
			events := c.handleQueue
			c.handleQueue = nil
			return events, true
		}

		if c.shutdown && len(c.sendQueue) == 0 && len(c.recvMap) == 0 {
			return nil, false
		}

		c.handleCond.Wait()
	}
}

func (c *Client) runHandler() {
	for {
		events, ok := c.getHandleEvents()
		if !ok {
			return
		}

		for _, e := range events {
			if e.req.callback != nil {
				e.req.callback(e.req.response, e.zxid, e.err)
			}
		}

	}
}

func (c *Client) setTimeouts(sessionTimeoutMs int32) {
	c.sessionTimeoutMs = sessionTimeoutMs
	sessionTimeout := time.Duration(sessionTimeoutMs) * time.Millisecond
	c.recvTimeout = sessionTimeout * 2 / 3
	c.pingInterval = c.recvTimeout / 2
}

func (c *Client) nextXid() int32 {
	return int32(atomic.AddUint32(&c.nextXidValue, 1) & 0x7fffffff)
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

func (c *Client) enqueueRequest(
	opCode int32, request any, response any,
	callback func(resp any, zxid int64, err error),
) {
	c.mut.Lock()
	defer c.mut.Unlock()

	req := clientRequest{
		xid:      c.nextXid(),
		opcode:   opCode,
		request:  request,
		response: response,

		callback: callback,
	}

	if c.state == StateHasSession {
		c.sendQueue = append(c.sendQueue, req)
		c.sendCond.Signal()
		return
	}

	err := ErrConnectionClosed
	if c.state == StateExpired {
		err = ErrSessionExpired
	}

	c.handleQueue = append(c.handleQueue, handleEvent{
		state: c.state,
		err:   err,
		req:   req,
	})
	c.handleCond.Signal()
}

func (c *Client) disconnect() {
	c.mut.Lock()
	defer c.mut.Unlock()

	c.state = StateDisconnected
	for _, req := range c.recvMap {
		c.handleQueue = append(c.handleQueue, handleEvent{
			state: StateDisconnected,
			err:   ErrConnectionClosed,
			req:   req,
		})
	}
	c.recvMap = map[int32]clientRequest{}

	for _, req := range c.sendQueue {
		c.handleQueue = append(c.handleQueue, handleEvent{
			state: StateDisconnected,
			err:   ErrConnectionClosed,
			req:   req,
		})
	}
	c.sendQueue = nil

	c.handleCond.Signal()
}

func (c *Client) sendData(conn tcpConn, req clientRequest) error {
	c.mut.Lock()
	// TODO Check Closed
	c.recvMap[req.xid] = req
	c.mut.Unlock()

	header := &requestHeader{req.xid, req.opcode}
	buf := c.writeCodec.buf[:]

	// encode header
	n, err := encodePacket(buf[4:], header)
	if err != nil {
		// TODO Handle Encode Error
		return nil
	}

	// encode request object
	n2, err := encodePacket(buf[4+n:], req.request)
	if err != nil {
		// TODO Handle Encode Error
		return nil
	}

	n += n2

	// write length to the first 4 bytes
	binary.BigEndian.PutUint32(buf[:4], uint32(n))

	_ = conn.SetWriteDeadline(c.recvTimeout)
	_, err = conn.Write(buf[:n+4])
	_ = conn.SetWriteDeadline(0)
	if err != nil {
		// TODO Handle Connection Error
		return err
	}

	return nil
}

func (c *Client) readSingleData(conn tcpConn) {
	buf := c.readCodec.buf

	// read package length
	_ = conn.SetReadDeadline(c.recvTimeout)
	_, err := io.ReadFull(conn, buf[:4])
	_ = conn.SetReadDeadline(0)
	if err != nil {
		// TODO check error
		return
	}

	blen := int(binary.BigEndian.Uint32(buf[:4]))
	if len(buf) < blen {
		// TODO Handle Len too Long
	}

	_ = conn.SetReadDeadline(c.recvTimeout)
	_, err = io.ReadFull(conn, buf[:blen])
	_ = conn.SetReadDeadline(0)
	if err != nil {
		// TODO Handle Error
		return
	}

	res := responseHeader{}
	_, err = decodePacket(buf[:16], &res)
	if err != nil {
		// TODO Handle Error
		return
	}

	if res.Xid == -1 {
		watchResp := &watcherEvent{}
		_, err = decodePacket(buf[16:blen], watchResp)
		if err != nil {
			// TODO Handle Decode Error
			return
		}
		ev := clientWatchEvent{
			Type:  watchResp.Type,
			State: watchResp.State,
			Path:  watchResp.Path,
			Err:   nil,
		}
		fmt.Println("EVENT:", ev)
		c.mut.Lock()
		c.handleQueue = append(c.handleQueue, handleEvent{
			state: c.state,
			zxid:  res.Zxid,
			req: clientRequest{
				xid:      -1,
				opcode:   opWatcherEvent,
				response: &ev,
			},
			err: res.Err.toError(),
		})
		c.handleCond.Signal()
		c.mut.Unlock()
		//c.sendEvent(ev)
		//c.notifyWatches(ev)
		return
	}
	if res.Xid == -2 {
		// Ping response. Ignore.
		return
	}

	if res.Xid < 0 {
		log.Printf("Xid < 0 (%d) but not ping or watcher event", res.Xid)
		return
	}

	c.mut.Lock()
	defer c.mut.Unlock()

	if res.Zxid > 0 {
		c.lastZxid = res.Zxid
	}

	req, ok := c.recvMap[res.Xid]
	if ok {
		delete(c.recvMap, res.Xid)
	}

	if !ok {
		log.Printf("Response for unknown request with xid %d", res.Xid)
		return
	}

	c.mut.Unlock()
	if res.Err != 0 {
		err = res.Err.toError()
	} else {
		_, err = decodePacket(buf[16:blen], req.response)
	}
	c.mut.Lock()

	c.handleQueue = append(c.handleQueue, handleEvent{
		state: c.state,
		zxid:  res.Zxid,
		req:   req,
		err:   err,
	})
	c.handleCond.Signal()
}
