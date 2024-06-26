package curator

import (
	"time"

	"github.com/QuangTung97/zk"
)

// ClientFactory for creating Client
type ClientFactory interface {
	Start(runner SessionRunner)
	Close()
}

// Client is a simpler interface for zookeeper client, mostly for mocking & faking for testing purpose
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
	Set(
		path string, data []byte, version int32,
		callback func(resp zk.SetResponse, err error),
	)
	Delete(path string, version int32, callback func(resp zk.DeleteResponse, err error))
}

type clientImpl struct {
	zkClient *zk.Client
	acl      []zk.ACL
}

// NewClientFactory creates a ClientFactory
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

	zkClient *zk.Client
}

func (f *clientFactoryImpl) Start(runner SessionRunner) {
	if f.zkClient != nil {
		panic("Start should only be called once")
	}

	auth := []byte(f.username + ":" + f.password)
	acl := zk.DigestACL(zk.PermAll, f.username, f.password)

	// channel will be closed when add auth completed (not need for the add auth callback to be finished)
	addAuthDone := make(chan struct{})

	zkClient, err := zk.NewClient(f.servers, 12*time.Second,
		zk.WithSessionEstablishedCallback(func(c *zk.Client) {
			<-addAuthDone
			runner.Begin(NewClient(c, acl))
		}),
		zk.WithReconnectingCallback(func(c *zk.Client) {
			runner.Retry()
		}),
		zk.WithSessionExpiredCallback(func(c *zk.Client) {
			runner.End()
		}),
	)
	if err != nil {
		panic(err)
	}
	f.zkClient = zkClient

	zkClient.AddAuth("digest", auth, func(resp zk.AddAuthResponse, err error) {})
	close(addAuthDone)
}

func (f *clientFactoryImpl) Close() {
	if f.zkClient != nil {
		f.zkClient.Close()
	}
}

// NewClient creates a Client
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

func (c *clientImpl) Set(
	path string, data []byte, version int32,
	callback func(resp zk.SetResponse, err error),
) {
	c.zkClient.Set(path, data, version, callback)
}

func (c *clientImpl) Delete(
	path string, version int32,
	callback func(resp zk.DeleteResponse, err error),
) {
	c.zkClient.Delete(path, version, callback)
}
