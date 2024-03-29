package zk

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"slices"
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
		c.connectAndRunTCPHandlers()
	}()

	go func() {
		defer c.wg.Done()
		c.runHandler()
	}()

	go func() {
		defer c.wg.Done()
		c.runPingLoop()
	}()

	return c, nil
}

// Client ...
type Client struct {
	logger Logger

	dialFunc          func(addr string, timeout time.Duration) (NetworkConn, error)
	dialRetryDuration time.Duration

	writeCodec    codecBuffer // not need to lock
	readCodec     codecBuffer // not need to lock
	authReadCodec codecBuffer // not need to lock

	selector ServerSelector

	sessEstablishedCallback func(c *Client)
	sessExpiredCallback     func(c *Client)
	reconnectingCallback    func(c *Client)

	// =================================
	// doesn't need to protect by mutex
	// =================================
	lastZxid int64
	passwd   []byte

	sessionTimeoutMs int32
	sessionID        int64

	recvTimeout  atomic.Int64
	pingInterval time.Duration
	// =================================

	// =================================
	// mutex protect following fields
	// =================================
	mut sync.Mutex

	nextXidValue uint32

	state State

	sendQueue    []clientRequest
	sendCond     *sync.Cond
	sendShutdown bool

	recvMap map[int32]clientRequest

	handleQueue    []handleEvent
	handleCond     *sync.Cond
	handleShutdown bool

	conn NetworkConn

	creds    []authCreds
	watchers map[watchPathType][]func(ev clientWatchEvent)
	// =================================

	wg sync.WaitGroup

	pingSignalChan chan struct{}
	pingCloseChan  chan struct{} // for closing ping loop
}

func (c *Client) getRecvTimeout() time.Duration {
	return time.Duration(c.recvTimeout.Load())
}

// Option ...
type Option func(c *Client)

func WithSessionEstablishedCallback(callback func(c *Client)) Option {
	return func(c *Client) {
		c.sessEstablishedCallback = callback
	}
}

func WithSessionExpiredCallback(callback func(c *Client)) Option {
	return func(c *Client) {
		c.sessExpiredCallback = callback
	}
}

func WithReconnectingCallback(callback func(c *Client)) Option {
	return func(c *Client) {
		c.reconnectingCallback = callback
	}
}

func WithDialRetryDuration(d time.Duration) Option {
	return func(c *Client) {
		c.dialRetryDuration = d
	}
}

func WithServerSelector(selector ServerSelector) Option {
	return func(c *Client) {
		c.selector = selector
	}
}

func WithDialTimeoutFunc(
	dialFunc func(addr string, timeout time.Duration) (NetworkConn, error),
) Option {
	return func(c *Client) {
		c.dialFunc = dialFunc
	}
}

func WithLogger(l Logger) Option {
	return func(c *Client) {
		c.logger = l
	}
}

type clientRequest struct {
	xid      int32
	opcode   int32
	request  any
	response any

	watch clientWatchRequest

	callback func(res any, zxid int64, err error)
}

type handleEvent struct {
	zxid int64
	err  error
	req  clientRequest
}

type clientWatchEvent struct {
	Type   EventType
	State  State
	Path   string // For non-session events, the path of the watched node.
	Err    error
	Server string // For connection events
}

type clientWatchRequest struct {
	pathType watchPathType
	callback func(ev clientWatchEvent)
}

type NetworkConn interface {
	io.Reader
	io.Writer
	io.Closer

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

	c := &Client{
		logger: &defaultLoggerImpl{},

		selector: NewServerListSelector(time.Now().UnixNano()),

		dialFunc: func(addr string, timeout time.Duration) (NetworkConn, error) {
			netConn, err := net.DialTimeout("tcp", addr, timeout)
			if err != nil {
				return nil, err
			}
			return NewTCPConn(netConn), nil
		},
		dialRetryDuration: 2 * connectTimeout,

		state:  StateDisconnected,
		passwd: emptyPassword,

		recvMap: map[int32]clientRequest{},

		watchers: map[watchPathType][]func(ev clientWatchEvent){},

		pingSignalChan: make(chan struct{}, 10),
		pingCloseChan:  make(chan struct{}),
	}

	for _, option := range options {
		option(c)
	}

	c.selector.Init(servers)

	c.sendCond = sync.NewCond(&c.mut)
	c.handleCond = sync.NewCond(&c.mut)

	c.setTimeouts(int32(sessionTimeout / time.Millisecond))

	return c, nil
}

func (c *Client) getFromSendQueue() ([]clientRequest, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	for {
		if c.state != StateHasSession {
			return nil, false
		}

		if len(c.sendQueue) > 0 {
			requests := c.sendQueue
			for _, req := range requests {
				if req.xid == pingRequestXid {
					continue
				}
				c.recvMap[req.xid] = req
			}
			c.sendQueue = nil
			return requests, true
		}

		if c.sendShutdown {
			return nil, false
		}

		c.sendCond.Wait()
	}
}

func (c *Client) disconnectAndCloseWhenShutdown() {
	conn, ok := c.disconnect(nil)
	if ok {
		_ = conn.Close()
	}
}

func (c *Client) tryToConnect() (NetworkConn, bool) {
	for {
		output := c.doConnect()
		if output.closed {
			c.disconnectAndCloseWhenShutdown()
			c.notifyHandleEventShutdown()
			return nil, false
		}

		if !output.needRetry {
			return output.conn, true
		}

		if output.withSleep {
			time.Sleep(c.dialRetryDuration)
		}
	}
}

const connectTimeout = 1 * time.Second

type connectOutput struct {
	conn      NetworkConn
	closed    bool
	needRetry bool
	withSleep bool
}

func (c *Client) notifyHandleEventShutdown() {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.handleShutdown = true
	c.handleCond.Signal()
}

func (c *Client) doConnect() connectOutput {
	c.mut.Lock()

	if c.sendShutdown {
		c.mut.Unlock()
		return connectOutput{
			closed: true,
		}
	}

	c.state = StateConnecting
	c.mut.Unlock()

	nextOutput := c.selector.Next()
	serverAddr := nextOutput.Server

	c.logger.Infof("Connecting to address: '%s'", serverAddr)

	conn, err := c.dialFunc(serverAddr, connectTimeout)
	if err != nil {
		c.mut.Lock()
		c.state = StateDisconnected
		c.mut.Unlock()
		c.logger.Warnf("Failed to connect to server: '%s', error: %v", serverAddr, err)
		return connectOutput{
			needRetry: true,
			withSleep: nextOutput.RetryStart,
		}
	}

	c.selector.NotifyConnected()
	c.logger.Infof("Connected to server: '%s'", serverAddr)

	c.mut.Lock()
	c.state = StateConnected
	c.mut.Unlock()

	err = c.authenticate(conn)
	if err != nil {
		c.mut.Lock()
		c.state = StateDisconnected
		c.mut.Unlock()
		_ = conn.Close()
		c.logger.Warnf("Failed to authenticate to server: '%s', error: %v", serverAddr, err)
		return connectOutput{
			needRetry: true,
		}
	}

	c.mut.Lock()
	defer c.mut.Unlock()

	return connectOutput{
		conn:   conn,
		closed: c.sendShutdown,
	}
}

func (c *Client) connectAndRunTCPHandlers() {
	for {
		conn, ok := c.tryToConnect()
		if !ok {
			return
		}

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			c.runSender(conn, &wg)
		}()

		go func() {
			defer wg.Done()
			c.runReceiver(conn, &wg)
		}()

		wg.Wait()
	}
}

func (c *Client) runSender(conn NetworkConn, wg *sync.WaitGroup) {
	for {
		requests, ok := c.getFromSendQueue()
		if !ok {
			return
		}

		select {
		case c.pingSignalChan <- struct{}{}:
		default:
		}

		for _, req := range requests {
			output := c.sendData(conn, req)
			if c.connectionRunnerStopped(output, wg) {
				return
			}
		}
	}
}

func (c *Client) connectionRunnerStopped(output connIOOutput, wg *sync.WaitGroup) bool {
	if output.closed {
		c.disconnectAndCloseWhenShutdown()
		return true
	}
	if output.broken {
		c.disconnectAndClose(output.err, wg)
		return true
	}
	return false
}

func (c *Client) runReceiver(conn NetworkConn, wg *sync.WaitGroup) {
	for {
		output := c.readSingleData(conn)
		if c.connectionRunnerStopped(output, wg) {
			return
		}
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

		if c.handleShutdown {
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
			c.handleEventCallback(e)
		}
	}
}

func (c *Client) getPingDuration() time.Duration {
	c.mut.Lock()
	duration := c.pingInterval
	c.mut.Unlock()
	return duration
}

func (c *Client) runPingLoop() {
	d := c.getPingDuration()
	timer := time.NewTimer(d)

	for {
		select {
		case <-timer.C:
			timer.Reset(c.getPingDuration())
			c.sendPingRequest()

		case <-c.pingSignalChan:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(c.getPingDuration())

		case <-c.pingCloseChan:
			return
		}
	}
}

func (c *Client) sendPingRequest() {
	c.enqueueRequest(
		opPing, &pingRequest{}, &pingResponse{},
		nil,
	)
}

func (c *Client) handleEventCallback(ev handleEvent) {
	if ev.req.callback != nil {
		ev.req.callback(ev.req.response, ev.zxid, ev.err)
	}
}

func (c *Client) setTimeouts(sessionTimeoutMs int32) {
	c.sessionTimeoutMs = sessionTimeoutMs
	sessionTimeout := time.Duration(sessionTimeoutMs) * time.Millisecond
	c.recvTimeout.Store(int64(sessionTimeout * 2 / 3))
	c.pingInterval = c.getRecvTimeout() / 2
}

func (c *Client) nextXid() int32 {
	c.nextXidValue++
	return int32(c.nextXidValue & 0x7fffffff)
}

func (c *Client) authenticate(conn NetworkConn) error {
	req := &connectRequest{
		ProtocolVersion: protocolVersion,
		LastZxidSeen:    c.lastZxid,
		TimeOut:         c.sessionTimeoutMs,
		SessionID:       c.sessionID,
		Passwd:          c.passwd,
	}

	authTimeout := c.getRecvTimeout() * 10

	// Encode and send connect request
	_ = conn.SetWriteDeadline(authTimeout)
	_, err := encodeObject[connectRequest](req, &c.writeCodec, conn)
	_ = conn.SetWriteDeadline(0)
	if err != nil {
		return err
	}

	// Receive and decode a connect response.
	r := connectResponse{}

	_ = conn.SetReadDeadline(authTimeout)
	err = decodeObject[connectResponse](&r, &c.authReadCodec, conn)
	_ = conn.SetReadDeadline(0)
	if err != nil {
		return err
	}

	if r.SessionID == 0 {
		c.mut.Lock()

		c.logger.Warnf("Session expired")

		c.sessionID = 0
		c.passwd = emptyPassword
		c.lastZxid = 0
		c.state = StateExpired

		if c.sessExpiredCallback != nil {
			c.appendHandleQueueGlobalEvent(c.sessExpiredCallback)
		}

		c.watchers = map[watchPathType][]func(ev clientWatchEvent){}

		c.mut.Unlock()

		return ErrSessionExpired
	}

	c.mut.Lock()
	prevIsZero := c.sessionID == 0
	c.sessionID = r.SessionID
	c.setTimeouts(r.TimeOut)
	c.passwd = r.Passwd
	c.state = StateHasSession
	c.conn = conn

	c.handleGlobalCallbacks(prevIsZero)

	c.reapplyAuthCreds()
	c.reapplyAllWatches()

	c.mut.Unlock()

	return nil
}

func (c *Client) reapplyAuthCreds() {
	for _, cred := range c.creds {
		c.enqueueAlreadyLocked(
			opSetAuth,
			&setAuthRequest{
				Type:   0,
				Scheme: cred.scheme,
				Auth:   cred.auth,
			},
			&setAuthResponse{},
			func(resp any, zxid int64, err error) {
			},
			clientWatchRequest{},
			false,
		)
	}
}

func (c *Client) appendHandleQueueGlobalEvent(callback func(c *Client)) {
	c.handleQueue = append(c.handleQueue, handleEvent{
		req: clientRequest{
			opcode:   opWatcherEvent,
			response: nil,
			callback: func(res any, zxid int64, err error) {
				callback(c)
			},
		},
	})
	c.handleCond.Signal()
}

func (c *Client) handleGlobalCallbacks(prevIsZero bool) {
	if c.sessEstablishedCallback != nil && prevIsZero {
		c.logger.Infof("Session established")
		c.appendHandleQueueGlobalEvent(c.sessEstablishedCallback)
	}

	if c.reconnectingCallback != nil && !prevIsZero {
		c.logger.Warnf("Connection is reconnected")
		c.appendHandleQueueGlobalEvent(c.reconnectingCallback)
	}
}

func (c *Client) reapplyAllWatches() {
	if len(c.watchers) == 0 {
		return
	}

	keys := make([]watchPathType, 0, len(c.watchers))
	for wpt := range c.watchers {
		keys = append(keys, wpt)
	}

	slices.SortFunc(keys, func(a, b watchPathType) int {
		if a.path < b.path {
			return -1
		}
		if a.path > b.path {
			return 1
		}
		return int(a.wType - b.wType)
	})

	const batchSize = 64
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		subKeys := keys[i:end]

		req := &setWatchesRequest{
			RelativeZxid: c.lastZxid,
		}

		for _, wpt := range subKeys {
			switch wpt.wType {
			case watchTypeExist:
				req.ExistWatches = append(req.ExistWatches, wpt.path)
			case watchTypeChild:
				req.ChildWatches = append(req.ChildWatches, wpt.path)
			case watchTypeData:
				req.DataWatches = append(req.DataWatches, wpt.path)
			default:
			}
		}

		c.enqueueAlreadyLocked(
			opSetWatches, req, &setWatchesResponse{},
			func(resp any, zxid int64, err error) {},
			clientWatchRequest{},
			false,
		)
	}
}

func (c *Client) enqueueRequest(
	opCode int32, request any, response any,
	callback func(resp any, zxid int64, err error),
) {
	c.enqueueRequestWithWatcher(
		opCode, request,
		response, callback, clientWatchRequest{},
	)
}

func (c *Client) enqueueRequestWithWatcher(
	opCode int32, request any, response any,
	callback func(resp any, zxid int64, err error),
	watch clientWatchRequest,
) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.sendShutdown {
		c.logger.Infof("Zookeeper client being accessed after Close()")
		return
	}

	c.enqueueAlreadyLocked(opCode, request, response, callback, watch, true)
}

const watchEventXid int32 = -1
const pingRequestXid int32 = -2

func (c *Client) addToWatcherMap(req clientRequest, err error) {
	if err != nil {
		if req.opcode != opExists {
			return
		}
		// if cmd = exists and err = ErrNoNode => add watch
		if !errors.Is(err, ErrNoNode) {
			return
		}
	}
	watch := req.watch
	pathType := watch.pathType
	if len(pathType.path) > 0 {
		c.watchers[pathType] = append(c.watchers[pathType], watch.callback)
	}
}

func (c *Client) enqueueAlreadyLocked(
	opCode int32, request any, response any,
	callback func(resp any, zxid int64, err error),
	watch clientWatchRequest, setAuth bool,
) {

	xid := pingRequestXid
	if opCode != opPing {
		xid = c.nextXid()
	}

	req := clientRequest{
		xid:      xid,
		opcode:   opCode,
		request:  request,
		response: response,

		watch: watch,

		callback: callback,
	}

	if setAuth && opCode == opSetAuth {
		r := request.(*setAuthRequest)
		c.creds = append(c.creds, authCreds{
			scheme: r.Scheme,
			auth:   r.Auth,
		})
	}

	if c.state == StateHasSession {
		c.sendQueue = append(c.sendQueue, req)
		c.sendCond.Signal()
		return
	}

	c.appendHandleQueue(req, ErrConnectionClosed)
}

func (c *Client) appendHandleQueue(req clientRequest, err error) {
	c.handleQueue = append(c.handleQueue, handleEvent{
		err: err,
		req: req,
	})
	c.handleCond.Signal()
}

func (c *Client) appendHandleQueueError(
	opCode int32, err error,
	callback func(res any, zxid int64, err error),
) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.appendHandleQueue(clientRequest{
		opcode:   opCode,
		callback: callback,
	}, err)
}

func (c *Client) disconnectAndClose(err error, wg *sync.WaitGroup) {
	conn, ok := c.disconnect(wg)
	if ok {
		c.logger.Warnf("Close connection with error: %v", err)
		_ = conn.Close()
	}
}

func (c *Client) updateStateAndFlushRequests(finalState State, wg *sync.WaitGroup) (NetworkConn, bool) {
	c.state = finalState
	oldConn := c.conn
	c.conn = nil

	events := make([]handleEvent, 0, len(c.recvMap)+len(c.sendQueue))

	for _, req := range c.recvMap {
		events = append(events, handleEvent{
			err: ErrConnectionClosed,
			req: req,
		})
	}
	c.recvMap = map[int32]clientRequest{}

	for _, req := range c.sendQueue {
		events = append(events, handleEvent{
			err: ErrConnectionClosed,
			req: req,
		})
	}
	c.sendQueue = nil

	slices.SortFunc(events, func(a, b handleEvent) int {
		return int(a.req.xid - b.req.xid)
	})

	c.handleQueue = append(c.handleQueue, events...)

	if wg != nil {
		wg.Add(1)
		c.handleQueue = append(c.handleQueue, handleEvent{
			req: clientRequest{
				opcode:   opWatcherEvent,
				response: nil,
				callback: func(res any, zxid int64, err error) {
					wg.Done()
				},
			},
		})
	}

	c.handleCond.Signal()
	c.sendCond.Signal() // notify when state is changed

	return oldConn, true
}

func (c *Client) disconnect(wg *sync.WaitGroup) (NetworkConn, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state != StateHasSession {
		return nil, false
	}

	return c.updateStateAndFlushRequests(StateDisconnected, wg)
}

type connIOOutput struct {
	closed bool
	broken bool
	err    error
}

func connError(err error) connIOOutput {
	return connIOOutput{
		broken: true,
		err:    err,
	}
}

func connNormal() connIOOutput {
	return connIOOutput{}
}

func (c *Client) sendData(conn NetworkConn, req clientRequest) connIOOutput {
	header := &requestHeader{Xid: req.xid, Opcode: req.opcode}
	buf := c.writeCodec.buf[:]

	// encode header
	n, err := encodePacket(buf[4:], header)
	if err != nil {
		return connError(err)
	}

	// encode request object
	n2, err := encodePacket(buf[4+n:], req.request)
	if err != nil {
		return connError(err)
	}

	n += n2

	// write length to the first 4 bytes
	binary.BigEndian.PutUint32(buf[:4], uint32(n))

	_ = conn.SetWriteDeadline(c.getRecvTimeout())
	_, err = conn.Write(buf[:n+4])
	_ = conn.SetWriteDeadline(0)
	if err != nil {
		return connError(err)
	}

	return connNormal()
}

func (c *Client) readSingleData(conn NetworkConn) connIOOutput {
	buf := c.readCodec.buf

	recvTimeout := c.getRecvTimeout()

	// read package length
	_ = conn.SetReadDeadline(recvTimeout)
	_, err := io.ReadFull(conn, buf[:4])
	if err != nil {
		return connError(err)
	}

	blen := int(binary.BigEndian.Uint32(buf[:4]))
	if len(buf) < blen {
		return connError(errors.New("message length too big"))
	}

	_ = conn.SetReadDeadline(recvTimeout)
	_, err = io.ReadFull(conn, buf[:blen])
	_ = conn.SetReadDeadline(0)
	if err != nil {
		return connError(err)
	}

	res := responseHeader{}
	_, err = decodePacket(buf[:16], &res)
	if err != nil {
		return connError(err)
	}

	if res.Zxid > 0 {
		c.lastZxid = res.Zxid
	}

	if res.Xid == watchEventXid {
		return c.handleWatchEvent(buf[:], blen, res)
	}
	if res.Xid == pingRequestXid {
		// Ping response. Ignore.
		return connNormal()
	}

	if res.Xid < 0 {
		c.logger.Warnf("Xid < 0 (%d) but not ping or watcher event", res.Xid)
		return connNormal()
	}

	return c.handleNormalResponse(res, buf[:], blen)
}

func (c *Client) handleNormalResponse(res responseHeader, buf []byte, blen int) connIOOutput {
	c.mut.Lock()
	defer c.mut.Unlock()

	req, ok := c.recvMap[res.Xid]
	if ok {
		delete(c.recvMap, res.Xid)
	}

	if !ok {
		c.logger.Warnf("Response for unknown request with xid %d", res.Xid)
		return connNormal()
	}

	c.mut.Unlock()

	// not need to decode in a mutex lock
	var err error
	if res.Err != 0 {
		err = res.Err.toError()
	} else {
		const responseHeaderSize = 16
		_, err = decodePacket(buf[responseHeaderSize:blen], req.response)
	}

	c.mut.Lock()

	if req.opcode == opClose {
		return connIOOutput{closed: true}
	}

	output := connNormal()
	if res.Err == errSessionExpired {
		err = ErrConnectionClosed
		output = connError(err)
	}

	c.addToWatcherMap(req, err)

	c.handleQueue = append(c.handleQueue, handleEvent{
		zxid: res.Zxid,
		req:  req,
		err:  err,
	})
	c.handleCond.Signal()

	return output
}

func (c *Client) handleWatchEvent(buf []byte, blen int, res responseHeader) connIOOutput {
	watchResp := &watcherEvent{}
	_, err := decodePacket(buf[16:blen], watchResp)
	if err != nil {
		return connError(err)
	}

	ev := clientWatchEvent{
		Type:  watchResp.Type,
		State: watchResp.State,
		Path:  watchResp.Path,
		Err:   nil,
	}

	c.mut.Lock()
	defer c.mut.Unlock()

	watchTypes := computeWatchTypes(watchResp.Type)
	var callbacks []func(ev clientWatchEvent)
	for _, wType := range watchTypes {
		wpt := watchPathType{path: ev.Path, wType: wType}
		callbacks = append(callbacks, c.watchers[wpt]...)
		delete(c.watchers, wpt)
	}

	c.handleQueue = append(c.handleQueue, handleEvent{
		zxid: res.Zxid,
		req: clientRequest{
			xid:      watchEventXid,
			opcode:   opWatcherEvent,
			response: &ev,
			callback: func(res any, zxid int64, err error) {
				ev := res.(*clientWatchEvent)
				for _, cb := range callbacks {
					cb(*ev)
				}
			},
		},
		err: res.Err.toError(),
	})
	c.handleCond.Signal()

	return connNormal()
}

// Close ...
func (c *Client) Close() {
	close(c.pingCloseChan)

	c.mut.Lock()
	c.sendShutdown = true
	c.enqueueAlreadyLocked(
		opClose, &closeRequest{}, &closeResponse{},
		nil, clientWatchRequest{},
		false,
	)
	c.mut.Unlock()

	c.wg.Wait()

	c.logger.Infof("Shutdown completed")
}

type CreateResponse struct {
	Zxid int64
	Path string
}

// Create ...
func (c *Client) Create(
	path string, data []byte, flags int32, acl []ACL,
	callback func(resp CreateResponse, err error),
) {
	handleCallback := func(resp any, zxid int64, err error) {
		if callback == nil {
			return
		}
		if err != nil {
			callback(CreateResponse{}, err)
			return
		}
		r := resp.(*createResponse)
		callback(CreateResponse{Path: r.Path, Zxid: zxid}, nil)
	}

	if err := ValidatePath(path, flags&FlagEphemeral != 0); err != nil {
		c.appendHandleQueueError(opCreate, err, handleCallback)
		return
	}

	c.enqueueRequest(
		opCreate,
		&CreateRequest{
			Path:  path,
			Data:  data,
			Flags: flags,
			Acl:   acl,
		},
		&createResponse{},
		handleCallback,
	)
}

type ChildrenResponse struct {
	Zxid     int64
	Children []string
}

type childrenOpts struct {
	watch         bool
	watchCallback func(ev Event)
}

type ChildrenOption func(opts *childrenOpts)

func WithChildrenWatch(callback func(ev Event)) ChildrenOption {
	return func(opts *childrenOpts) {
		if callback == nil {
			return
		}
		opts.watch = true
		opts.watchCallback = callback
	}
}

func (c *Client) Children(
	path string,
	callback func(resp ChildrenResponse, err error),
	options ...ChildrenOption,
) {
	handleCallback := func(resp any, zxid int64, err error) {
		if callback == nil {
			return
		}
		if err != nil {
			callback(ChildrenResponse{}, err)
			return
		}
		r := resp.(*getChildren2Response)
		callback(ChildrenResponse{
			Zxid:     zxid,
			Children: r.Children,
		}, nil)
	}

	if err := ValidatePath(path, false); err != nil {
		c.appendHandleQueueError(opGetChildren2, err, handleCallback)
		return
	}

	opts := childrenOpts{
		watch: false,
	}
	for _, fn := range options {
		fn(&opts)
	}

	watch := clientWatchRequest{}
	if opts.watch {
		watch = clientWatchRequest{
			pathType: watchPathType{
				path:  path,
				wType: watchTypeChild,
			},
			callback: func(ev clientWatchEvent) {
				opts.watchCallback(Event{
					Type:   ev.Type,
					State:  ev.State,
					Path:   ev.Path,
					Err:    ev.Err,
					Server: ev.Server,
				})
			},
		}
	}

	c.enqueueRequestWithWatcher(
		opGetChildren2,
		&getChildren2Request{
			Path:  path,
			Watch: opts.watch,
		},
		&getChildren2Response{},
		handleCallback,
		watch,
	)
}

type GetResponse struct {
	Zxid int64
	Data []byte
	Stat Stat
}

type getOpts struct {
	watch         bool
	watchCallback func(ev Event)
}

type GetOption func(opts *getOpts)

func WithGetWatch(callback func(ev Event)) GetOption {
	return func(opts *getOpts) {
		if callback == nil {
			return
		}
		opts.watch = true
		opts.watchCallback = callback
	}
}

func (c *Client) Get(
	path string,
	callback func(resp GetResponse, err error),
	options ...GetOption,
) {
	handleCallback := func(resp any, zxid int64, err error) {
		if callback == nil {
			return
		}
		if err != nil {
			callback(GetResponse{}, err)
			return
		}
		r := resp.(*getDataResponse)
		callback(GetResponse{
			Zxid: zxid,
			Data: r.Data,
			Stat: r.Stat,
		}, nil)
	}

	if err := ValidatePath(path, false); err != nil {
		c.appendHandleQueueError(opGetData, err, handleCallback)
		return
	}

	opts := getOpts{
		watch: false,
	}
	for _, fn := range options {
		fn(&opts)
	}

	watch := clientWatchRequest{}
	if opts.watch {
		watch = clientWatchRequest{
			pathType: watchPathType{
				path:  path,
				wType: watchTypeData,
			},
			callback: func(ev clientWatchEvent) {
				opts.watchCallback(Event{
					Type:   ev.Type,
					State:  ev.State,
					Path:   ev.Path,
					Err:    ev.Err,
					Server: ev.Server,
				})
			},
		}
	}

	c.enqueueRequestWithWatcher(
		opGetData,
		&getDataRequest{
			Path:  path,
			Watch: opts.watch,
		},
		&getDataResponse{},
		handleCallback,
		watch,
	)
}

type SetResponse struct {
	Zxid int64
	Stat Stat
}

func (c *Client) Set(
	path string, data []byte, version int32,
	callback func(resp SetResponse, err error),
) {
	c.enqueueRequest(
		opSetData,
		&SetDataRequest{
			Path:    path,
			Data:    data,
			Version: version,
		},
		&setDataResponse{},
		func(resp any, zxid int64, err error) {
			if callback == nil {
				return
			}
			if err != nil {
				callback(SetResponse{}, err)
				return
			}
			r := resp.(*setDataResponse)
			callback(SetResponse{
				Zxid: zxid,
				Stat: r.Stat,
			}, nil)
		},
	)
}

type ExistsResponse struct {
	Zxid int64
	Stat Stat
}

type existsOpts struct {
	watch         bool
	watchCallback func(ev Event)
}

type ExistsOption func(opts *existsOpts)

func WithExistsWatch(callback func(ev Event)) ExistsOption {
	return func(opts *existsOpts) {
		if callback == nil {
			return
		}
		opts.watch = true
		opts.watchCallback = callback
	}
}

func (c *Client) Exists(
	path string,
	callback func(resp ExistsResponse, err error),
	options ...ExistsOption,
) {
	handleCallback := func(resp any, zxid int64, err error) {
		if callback == nil {
			return
		}
		if err != nil {
			callback(ExistsResponse{}, err)
			return
		}
		r := resp.(*existsResponse)
		callback(ExistsResponse{
			Zxid: zxid,
			Stat: r.Stat,
		}, nil)
	}

	if err := ValidatePath(path, false); err != nil {
		c.appendHandleQueueError(opExists, err, handleCallback)
		return
	}

	opts := existsOpts{
		watch: false,
	}
	for _, fn := range options {
		fn(&opts)
	}

	watch := clientWatchRequest{}
	if opts.watch {
		watch = clientWatchRequest{
			pathType: watchPathType{
				path:  path,
				wType: watchTypeExist,
			},
			callback: func(ev clientWatchEvent) {
				opts.watchCallback(Event{
					Type:   ev.Type,
					State:  ev.State,
					Path:   ev.Path,
					Err:    ev.Err,
					Server: ev.Server,
				})
			},
		}
	}

	c.enqueueRequestWithWatcher(
		opExists,
		&existsRequest{
			Path:  path,
			Watch: opts.watch,
		},
		&existsResponse{},
		handleCallback,
		watch,
	)
}

type DeleteResponse struct {
	Zxid int64
}

func (c *Client) Delete(
	path string, version int32,
	callback func(resp DeleteResponse, err error),
) {
	handleCallback := func(resp any, zxid int64, err error) {
		if callback == nil {
			return
		}
		if err != nil {
			callback(DeleteResponse{}, err)
			return
		}
		callback(DeleteResponse{
			Zxid: zxid,
		}, nil)
	}

	if err := ValidatePath(path, false); err != nil {
		c.appendHandleQueueError(opDelete, err, handleCallback)
		return
	}

	c.enqueueRequest(
		opDelete,
		&DeleteRequest{
			Path:    path,
			Version: version,
		},
		&deleteResponse{},
		handleCallback,
	)
}

type AddAuthResponse struct {
	Zxid int64
}

// AddAuth often used with "digest" scheme and auth = "username:password" (password is not hashed)
func (c *Client) AddAuth(
	scheme string, auth []byte,
	callback func(resp AddAuthResponse, err error),
) {
	c.enqueueRequest(
		opSetAuth,
		&setAuthRequest{
			Type:   0,
			Scheme: scheme,
			Auth:   auth,
		},
		&setAuthResponse{},
		func(resp any, zxid int64, err error) {
			if callback == nil {
				return
			}
			if err != nil {
				callback(AddAuthResponse{}, err)
				return
			}
			callback(AddAuthResponse{
				Zxid: zxid,
			}, nil)
		},
	)
}

type SetACLResponse struct {
	Zxid int64
	Stat Stat
}

// SetACL set ACL to ZK
// version is the ACL Version (Stat.Aversion), not a normal version number
func (c *Client) SetACL(
	path string, acl []ACL, version int32,
	callback func(resp SetACLResponse, err error),
) {
	handleCallback := func(resp any, zxid int64, err error) {
		if callback == nil {
			return
		}
		if err != nil {
			callback(SetACLResponse{}, err)
			return
		}
		r := resp.(*setAclResponse)
		callback(SetACLResponse{
			Zxid: zxid,
			Stat: r.Stat,
		}, nil)
	}

	if err := ValidatePath(path, false); err != nil {
		c.appendHandleQueueError(opSetAcl, err, handleCallback)
		return
	}

	c.enqueueRequest(
		opSetAcl,
		&setAclRequest{
			Path:    path,
			Acl:     acl,
			Version: version,
		},
		&setAclResponse{},
		handleCallback,
	)
}

type GetACLResponse struct {
	Zxid int64
	ACL  []ACL
	Stat Stat
}

// GetACL returns ACL for a znode
func (c *Client) GetACL(
	path string,
	callback func(resp GetACLResponse, err error),
) {
	handleCallback := func(resp any, zxid int64, err error) {
		if callback == nil {
			return
		}
		if err != nil {
			callback(GetACLResponse{}, err)
			return
		}
		r := resp.(*getAclResponse)
		callback(GetACLResponse{
			Zxid: zxid,
			ACL:  r.Acl,
			Stat: r.Stat,
		}, nil)
	}

	if err := ValidatePath(path, false); err != nil {
		c.appendHandleQueueError(opGetAcl, err, handleCallback)
		return
	}

	c.enqueueRequest(
		opGetAcl,
		&getAclRequest{
			Path: path,
		},
		&getAclResponse{},
		handleCallback,
	)
}
