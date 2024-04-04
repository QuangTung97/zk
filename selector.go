package zk

import (
	"math/rand"
	"sync"
)

// SelectNextOutput ...
type SelectNextOutput struct {
	Server     string
	RetryStart bool // equal to true if the client should sleep after retry to the next server
}

// ServerSelector for selecting servers for connecting
type ServerSelector interface {
	// Init the selector
	Init(servers []string)
	// Next choose the next server for connecting
	Next() SelectNextOutput
	// NotifyConnected notify the selector for retrying the same address later after successfully connected
	NotifyConnected()
}

// ServerListSelector ...
type ServerListSelector struct {
	mut          sync.Mutex
	servers      []string
	lastIndex    int
	numNextCalls int
	rand         *rand.Rand
	notified     bool
}

// NewServerListSelector ...
func NewServerListSelector(seed int64) ServerSelector {
	return &ServerListSelector{
		rand: rand.New(rand.NewSource(seed)),
	}
}

// Init ...
func (s *ServerListSelector) Init(servers []string) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.servers = FormatServers(servers)
	stringShuffleRand(s.servers, s.rand)
	s.lastIndex = -1
	s.numNextCalls = 0
}

// Next ...
func (s *ServerListSelector) Next() SelectNextOutput {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.notified = false

	s.lastIndex++
	s.lastIndex = s.lastIndex % len(s.servers)
	s.numNextCalls++

	retryStart := false
	if s.numNextCalls >= len(s.servers) {
		retryStart = true
	}

	return SelectNextOutput{
		Server:     s.servers[s.lastIndex],
		RetryStart: retryStart,
	}
}

// NotifyConnected ...
func (s *ServerListSelector) NotifyConnected() {
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.notified {
		return
	}
	s.lastIndex--
	s.notified = true
	s.numNextCalls = 0
}
