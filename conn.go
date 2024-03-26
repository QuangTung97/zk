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

// Event is an Znode event sent by the server.
// Refer to EventType for more details.
type Event struct {
	Type   EventType
	State  State
	Path   string // For non-session events, the path of the watched node.
	Err    error
	Server string // For connection events
}

// HostProvider is used to represent a set of hosts a ZooKeeper client should connect to.
// It is an analog of the Java equivalent:
// http://svn.apache.org/viewvc/zookeeper/trunk/src/java/main/org/apache/zookeeper/client/HostProvider.java?view=markup
type HostProvider interface {
	// Init is called first, with the servers specified in the connection string.
	Init(servers []string) error
	// Len returns the number of servers.
	Len() int
	// Next returns the next server to connect to. retryStart will be true if we've looped through
	// all known servers without Connected() being called.
	Next() (server string, retryStart bool)
	// Notify the HostProvider of a successful connection.
	Connected()
}
