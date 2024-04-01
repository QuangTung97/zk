// Package zk is a native Go client library for the ZooKeeper orchestration service.
package zk

import (
	"errors"
)

// ErrInvalidPath indicates that an operation was being attempted on
// an invalid path. (e.g. empty path).
var ErrInvalidPath = errors.New("zk: invalid path")

const (
	bufferSize = 1536 * 1024
)

type watchType int

const (
	watchTypeData watchType = iota
	watchTypeExist
	watchTypeChild
)

type watchPathType struct {
	path  string
	wType watchType
}

type authCreds struct {
	scheme string
	auth   []byte
}

// Event is a Znode event sent by the server.
// Refer to EventType for more details.
type Event struct {
	Type  EventType
	State State
	Path  string // For non-session events, the path of the watched node.
}
