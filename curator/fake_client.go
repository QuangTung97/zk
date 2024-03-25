package curator

import (
	"fmt"
	stdpath "path"
	"slices"

	"github.com/QuangTung97/zk"
)

type FakeSessionState struct {
	HasSession bool
}

type FakeClientID string

type ZNode struct {
	Name     string
	Data     []byte
	Flags    int32
	Children []*ZNode
}

type FakeZookeeper struct {
	States   map[FakeClientID]*FakeSessionState
	Sessions map[FakeClientID][]SessionCallback
	Clients  map[FakeClientID]Client
	Pending  map[FakeClientID]map[string][]any

	Root *ZNode // root znode

	Zxid int64
}

func NewFakeZookeeper() *FakeZookeeper {
	return &FakeZookeeper{
		States:   map[FakeClientID]*FakeSessionState{},
		Sessions: map[FakeClientID][]SessionCallback{},
		Clients:  map[FakeClientID]Client{},
		Pending:  map[FakeClientID]map[string][]any{},

		Root: &ZNode{},
		Zxid: 100,
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
const CallTypeCreate = "create"

func computePathNodes(pathValue string) []string {
	var nodes []string
	for {
		baseVal := stdpath.Base(pathValue)
		nodes = append(nodes, baseVal)
		if baseVal == "/" {
			break
		}
		pathValue = stdpath.Dir(pathValue)
	}
	slices.Reverse(nodes)
	return nodes
}

func (s *FakeZookeeper) findParentNode(pathValue string) *ZNode {
	nodes := computePathNodes(pathValue)
	current := s.Root
	for _, n := range nodes[1 : len(nodes)-1] {
		for _, child := range current.Children {
			if child.Name == n {
				current = child
				continue
			}
		}
		return nil
	}
	return current
}

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
	values := make([]string, 0)
	for p := range s.Pending[clientID] {
		values = append(values, p)
	}
	slices.Sort(values)
	return values
}

func (s *FakeZookeeper) CreateCalls(clientID FakeClientID) []CreateInput {
	m := s.Pending[clientID]
	inputs := m[CallTypeCreate]
	if len(inputs) == 0 {
		panic("No create call currently pending")
	}
	var res []CreateInput
	for _, input := range inputs {
		res = append(res, input.(CreateInput))
	}
	return res
}

func (s *FakeZookeeper) CreateApply(clientID FakeClientID) {
	calls := s.CreateCalls(clientID)
	for _, c := range calls {
		parent := s.findParentNode(c.Path)
		parent.Children = append(parent.Children, &ZNode{
			Name:  stdpath.Base(c.Path),
			Data:  c.Data,
			Flags: c.Flags,
		})
		s.Zxid++
		c.Callback(zk.CreateResponse{
			Zxid: s.Zxid,
			Path: c.Path,
		}, nil)
	}
	delete(s.Pending[clientID], CallTypeCreate)
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

func (c *fakeClient) getPendingMap() map[string][]any {
	m, ok := c.store.Pending[c.clientID]
	if !ok {
		m = map[string][]any{}
		c.store.Pending[c.clientID] = m
	}
	return m
}

func (c *fakeClient) Children(path string, callback func(resp zk.ChildrenResponse, err error)) {
	m := c.getPendingMap()
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

type CreateInput struct {
	Path     string
	Data     []byte
	Flags    int32
	Callback func(resp zk.CreateResponse, err error)
}

func (c *fakeClient) Create(
	path string, data []byte, flags int32,
	callback func(resp zk.CreateResponse, err error),
) {
	m := c.getPendingMap()
	m[CallTypeCreate] = append(m[CallTypeCreate], CreateInput{
		Path:     path,
		Data:     data,
		Flags:    flags,
		Callback: callback,
	})
}
