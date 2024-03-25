package curator

import (
	"fmt"
	"slices"

	"github.com/QuangTung97/zk"
)

type FakeSessionState struct {
	HasSession bool
}

type FakeClientID string

type FakeZookeeper struct {
	States   map[FakeClientID]*FakeSessionState
	Sessions map[FakeClientID][]SessionCallback
	Clients  map[FakeClientID]Client
	Pending  map[FakeClientID]map[string][]any
}

func NewFakeZookeeper() *FakeZookeeper {
	return &FakeZookeeper{
		States:   map[FakeClientID]*FakeSessionState{},
		Sessions: map[FakeClientID][]SessionCallback{},
		Clients:  map[FakeClientID]Client{},
		Pending:  map[FakeClientID]map[string][]any{},
	}
}

type fakeClientFactory struct {
	store    *FakeZookeeper
	clientID FakeClientID
}

func NewFakeClientFactory(store *FakeZookeeper, clientID FakeClientID) ClientFactory {
	store.States[clientID] = &FakeSessionState{
		HasSession: false,
	}

	return &fakeClientFactory{
		store:    store,
		clientID: clientID,
	}
}

func (c *fakeClientFactory) Start(callbacks ...SessionCallback) {
	c.store.Sessions[c.clientID] = callbacks
	c.store.Clients[c.clientID] = &fakeClient{
		store:    c.store,
		clientID: c.clientID,
	}
}

func (s *FakeZookeeper) Begin(clientID FakeClientID) {
	client := s.Clients[clientID]
	callbacks := s.Sessions[clientID]
	for _, cb := range callbacks {
		cb.Begin(client)
	}
}

type ChildrenInput struct {
	Path     string
	Callback func(resp zk.ChildrenResponse, err error)
}

const CallTypeChildren = "children"

func (s *FakeZookeeper) ChildrenCalls(clientID FakeClientID) []ChildrenInput {
	m := s.Pending[clientID]
	inputs := m[CallTypeChildren]
	if len(inputs) == 0 {
		panic("No children call currently pending")
	}
	var res []ChildrenInput
	for _, input := range inputs {
		res = append(res, input.(ChildrenInput))
	}
	return res
}

func (s *FakeZookeeper) ChildrenCall(clientID FakeClientID) ChildrenInput {
	inputs := s.ChildrenCalls(clientID)
	if len(inputs) > 1 {
		panic("ChildrenCall has more than one call")
	}
	return inputs[0]
}

func (s *FakeZookeeper) PrintPendingCalls() {
	for client, m := range s.Pending {
		if len(m) == 0 {
			continue
		}
		fmt.Println("------------------------------------------------")
		fmt.Println("CLIENT:", client)
		for p, inputs := range m {
			fmt.Println("  ", p, inputs)
		}
	}
	if len(s.Pending) > 0 {
		fmt.Println("------------------------------------------------")
	}
}

func (s *FakeZookeeper) PendingCalls(clientID FakeClientID) []string {
	var values []string
	for p := range s.Pending[clientID] {
		values = append(values, p)
	}
	slices.Sort(values)
	return values
}

type fakeClient struct {
	store    *FakeZookeeper
	clientID FakeClientID
}

func (c *fakeClient) Get(path string, callback func(resp zk.GetResponse, err error)) {
}

func (c *fakeClient) GetW(path string,
	callback func(resp zk.GetResponse, err error),
	watcher func(ev zk.Event),
) {
}

func (c *fakeClient) Children(path string, callback func(resp zk.ChildrenResponse, err error)) {
	m, ok := c.store.Pending[c.clientID]
	if !ok {
		m = map[string][]any{}
		c.store.Pending[c.clientID] = m
	}

	m[CallTypeChildren] = append(m[CallTypeChildren], ChildrenInput{
		Path:     path,
		Callback: callback,
	})
}

func (c *fakeClient) ChildrenW(path string,
	callback func(resp zk.ChildrenResponse, err error),
	watcher func(ev zk.Event),
) {
}

func (c *fakeClient) Create(
	path string, data []byte, flags int32,
	callback func(resp zk.CreateResponse, err error),
) {
}
