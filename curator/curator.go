package curator

// Curator is used for maintaining Session object.
// when new zookeeper session is established, old Session will be invalided and can NOT be used anymore
type Curator struct {
	initFunc func(sess *Session)

	client Client
	sess   *Session
}

// SessionRunner is an interface that be implemented by Curator
type SessionRunner interface {
	Begin(client Client)
	Retry()
	End()
}

// Session represents a zookeeper session
type Session struct {
	retryFuncs []func(sess *Session)
	state      *Curator
}

// New creates a Curator with simple init function when session started
func New(
	initFunc func(sess *Session),
) *Curator {
	return &Curator{
		initFunc: initFunc,
	}
}

// SessionCallback ...
type SessionCallback func(sess *Session, next func(sess *Session))

// NewChain creates a chain of callbacks when next callback is called only after the previous callback allows.
// For example when doing locking, ONLY after the lock is granted the next callback could allow to run.
func NewChain(
	initFuncList ...SessionCallback,
) *Curator {
	next := func(sess *Session) {}
	for i := len(initFuncList) - 1; i >= 0; i-- {
		initFn := initFuncList[i]
		oldNext := next
		next = func(sess *Session) {
			initFn(sess, oldNext)
		}
	}
	return &Curator{
		initFunc: next,
	}
}

// Begin callback when new session is established
func (c *Curator) Begin(client Client) {
	c.client = client
	c.sess = &Session{
		state: c,
	}
	c.initFunc(c.sess)
}

// Retry callback when new connection is established after disconnecting
func (c *Curator) Retry() {
	if c.sess == nil {
		return
	}
	for _, cb := range c.sess.retryFuncs {
		cb(c.sess)
	}
	c.sess.retryFuncs = nil
}

// End callback when current session is expired
func (c *Curator) End() {
	c.sess = nil
}

// GetClient returns Client
func (s *Session) GetClient() Client {
	return s.state.client
}

// AddRetry add a callback function that will be called after connection is re-established.
func (s *Session) AddRetry(callback func(sess *Session)) {
	s.retryFuncs = append(s.retryFuncs, callback)
}
