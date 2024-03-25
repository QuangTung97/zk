package curator

import (
	"github.com/QuangTung97/zk"
)

type ChildrenInput struct {
	Path     string
	Callback func(resp zk.ChildrenResponse, err error)
	Watch    bool
	Watcher  func(ev zk.Event)
}

type CreateInput struct {
	Path     string
	Data     []byte
	Flags    int32
	Callback func(resp zk.CreateResponse, err error)
}

type RetryInput struct {
}
