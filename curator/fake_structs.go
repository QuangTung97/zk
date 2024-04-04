package curator

import (
	"github.com/QuangTung97/zk"
)

// ChildrenInput ...
type ChildrenInput struct {
	Path     string
	Callback func(resp zk.ChildrenResponse, err error)
	Watch    bool
	Watcher  func(ev zk.Event)
}

// CreateInput ...
type CreateInput struct {
	Path     string
	Data     []byte
	Flags    int32
	Callback func(resp zk.CreateResponse, err error)
}

// GetInput ...
type GetInput struct {
	Path     string
	Callback func(resp zk.GetResponse, err error)
	Watch    bool
	Watcher  func(ev zk.Event)
}

// SetInput ...
type SetInput struct {
	Path     string
	Data     []byte
	Version  int32
	Callback func(resp zk.SetResponse, err error)
}

// DeleteInput ...
type DeleteInput struct {
	Path     string
	Version  int32
	Callback func(resp zk.DeleteResponse, err error)
}

// RetryInput ...
type RetryInput struct {
}
