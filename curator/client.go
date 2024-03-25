package curator

import (
	"time"

	"github.com/QuangTung97/zk"
)

type SessionCallback interface {
	Begin(client Client)
	Retry()
	End()
}

type ClientFactory interface {
	Start(callbacks ...SessionCallback)
}

type Client interface {
	Get(path string, callback func(resp zk.GetResponse, err error))
	GetW(path string,
		callback func(resp zk.GetResponse, err error),
		watcher func(ev zk.Event),
	)

	Children(path string, callback func(resp zk.ChildrenResponse, err error))
	ChildrenW(path string,
		callback func(resp zk.ChildrenResponse, err error),
		watcher func(ev zk.Event),
	)

	Create(
		path string, data []byte, flags int32,
		callback func(resp zk.CreateResponse, err error),
	)
}

type clientImpl struct {
	zkClient *zk.Client
	acl      []zk.ACL
}

func NewClientFactory(servers []string, username string, password string) ClientFactory {
	return &clientFactoryImpl{
		servers:  servers,
		username: username,
		password: password,
	}
}

type clientFactoryImpl struct {
	servers []string

	username string
	password string
}

func (f *clientFactoryImpl) Start(callbacks ...SessionCallback) {
	auth := []byte(f.username + ":" + f.password)
	acl := zk.DigestACL(zk.PermAll, f.username, f.password)

	// channel will be closed when add auth completed (not need for the callback to be finished)
	addAuthDone := make(chan struct{})

	zkClient, err := zk.NewClient(f.servers, 12*time.Second,
		zk.WithSessionEstablishedCallback(func(c *zk.Client) {
			<-addAuthDone
			for _, cb := range callbacks {
				cb.Begin(NewClient(c, acl))
			}
		}),
		zk.WithReconnectingCallback(func(c *zk.Client) {
			for _, cb := range callbacks {
				cb.Retry()
			}
		}),
		zk.WithSessionExpiredCallback(func(c *zk.Client) {
			for _, cb := range callbacks {
				cb.End()
			}
		}),
	)
	if err != nil {
		panic(err)
	}

	zkClient.AddAuth("digest", auth, func(resp zk.AddAuthResponse, err error) {})
	addAuthDone <- struct{}{}
}

func NewClient(zkClient *zk.Client, acl []zk.ACL) Client {
	return &clientImpl{
		zkClient: zkClient,
		acl:      acl,
	}
}

func (c *clientImpl) Get(path string, callback func(resp zk.GetResponse, err error)) {
	c.zkClient.Get(path, callback)
}

func (c *clientImpl) GetW(path string,
	callback func(resp zk.GetResponse, err error),
	watcher func(ev zk.Event),
) {
	c.zkClient.Get(path, callback, zk.WithGetWatch(watcher))
}

func (c *clientImpl) Children(path string, callback func(resp zk.ChildrenResponse, err error)) {
	c.zkClient.Children(path, callback)
}

func (c *clientImpl) ChildrenW(path string,
	callback func(resp zk.ChildrenResponse, err error),
	watcher func(ev zk.Event),
) {
	c.zkClient.Children(path, callback, zk.WithChildrenWatch(watcher))
}

func (c *clientImpl) Create(
	path string, data []byte, flags int32,
	callback func(resp zk.CreateResponse, err error),
) {
	c.zkClient.Create(path, data, flags, c.acl, callback)
}
