package zk

import (
	"math/rand"
	"sync"
)

type SelectNextOutput struct {
	Server     string
	RetryStart bool
}

type ServerSelector interface {
	Init(servers []string)
	Next() SelectNextOutput
	NotifyConnected()
}

type ServerListSelector struct {
	mut          sync.Mutex
	servers      []string
	lastIndex    int
	numNextCalls int
	rand         *rand.Rand
	notified     bool
}

func NewServerListSelector(seed int64) ServerSelector {
	return &ServerListSelector{
		rand: rand.New(rand.NewSource(seed)),
	}
}

func (s *ServerListSelector) Init(servers []string) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.servers = FormatServers(servers)
	stringShuffleRand(s.servers, s.rand)
	s.lastIndex = -1
	s.numNextCalls = 0
}

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
