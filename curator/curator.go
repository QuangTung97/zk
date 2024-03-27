package curator

type Curator struct {
	initFunc func(sess *Session)

	client Client
	sess   *Session
}

type SessionRunner interface {
	Begin(client Client)
	Retry()
	End()
}

type Session struct {
	retryFuncs []func(sess *Session)
	state      *Curator
}

func New(
	initFunc func(sess *Session),
) *Curator {
	return &Curator{
		initFunc: initFunc,
	}
}

type SessionCallback func(sess *Session, next func(sess *Session))

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

func (c *Curator) Begin(client Client) {
	c.client = client
	c.sess = &Session{
		state: c,
	}
	c.initFunc(c.sess)
}

func (c *Curator) Retry() {
	if c.sess == nil {
		return
	}
	for _, cb := range c.sess.retryFuncs {
		cb(c.sess)
	}
	c.sess.retryFuncs = nil
}

func (c *Curator) End() {
	c.sess = nil
}

type nullClient struct {
	valid  bool
	client Client
}

func (s *Session) getClient() nullClient {
	if s.state.sess != s {
		return nullClient{}
	}
	return nullClient{
		valid:  true,
		client: s.state.client,
	}
}

func (s *Session) Run(fn func(client Client)) {
	sessClient := s.getClient()
	if !sessClient.valid {
		return
	}
	fn(sessClient.client)
}

func (s *Session) AddRetry(callback func(sess *Session)) {
	s.retryFuncs = append(s.retryFuncs, callback)
}
