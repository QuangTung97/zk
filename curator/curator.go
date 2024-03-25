package curator

type Curator struct {
	initFunc func(sess *Session)

	client Client
	sess   *Session
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

func (c *Curator) Begin(client Client) {
	c.client = client
	c.sess = &Session{
		state: c,
	}
	c.initFunc(c.sess)
}

func (c *Curator) Retry() {
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
