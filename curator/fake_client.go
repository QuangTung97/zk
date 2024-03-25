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
	Pending  map[FakeClientID][]any

	Root *ZNode // root znode

	Zxid int64
}

func NewFakeZookeeper() *FakeZookeeper {
	return &FakeZookeeper{
		States:   map[FakeClientID]*FakeSessionState{},
		Sessions: map[FakeClientID][]SessionCallback{},
		Clients:  map[FakeClientID]Client{},
		Pending:  map[FakeClientID][]any{},

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
	for i := 1; i < len(nodes)-1; i++ {
		n := nodes[i]
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

func getActionWithType[T any](s *FakeZookeeper, clientID FakeClientID, methodName string) T {
	actions := s.Pending[clientID]
	msg := fmt.Sprintf("No %s call currently pending", methodName)
	if len(actions) == 0 {
		panic(msg)
	}
	val, ok := actions[0].(T)
	if !ok {
		panic(msg)
	}

	s.popFirst(clientID)

	return val
}

func (s *FakeZookeeper) PrintPendingCalls() {
	for client, m := range s.Pending {
		if len(m) == 0 {
			continue
		}
		fmt.Println("------------------------------------------------")
		fmt.Println("CLIENT:", client)
		for _, inputs := range m {
			fmt.Println("  ", inputs)
		}
	}
	if len(s.Pending) > 0 {
		fmt.Println("------------------------------------------------")
	}
}

func (s *FakeZookeeper) PendingCalls(clientID FakeClientID) []string {
	values := make([]string, 0)
	for _, input := range s.Pending[clientID] {
		switch input.(type) {
		case ChildrenInput:
			values = append(values, "children")
		case CreateInput:
			values = append(values, "create")
		case RetryInput:
			values = append(values, "retry")
		default:
			panic("Unknown input type")
		}
	}
	return values
}

func (s *FakeZookeeper) CreateCall(clientID FakeClientID) CreateInput {
	return getActionWithType[CreateInput](s, clientID, "Create")
}

func (s *FakeZookeeper) popFirst(clientID FakeClientID) {
	actions := s.Pending[clientID]
	actions = slices.Clone(actions[1:])
	s.Pending[clientID] = actions
}

func (s *FakeZookeeper) CreateApply(clientID FakeClientID) {
	input := s.CreateCall(clientID)
	parent := s.findParentNode(input.Path)
	parent.Children = append(parent.Children, &ZNode{
		Name:  stdpath.Base(input.Path),
		Data:  input.Data,
		Flags: input.Flags,
	})
	s.Zxid++
	input.Callback(zk.CreateResponse{
		Zxid: s.Zxid,
		Path: input.Path,
	}, nil)
}

func (s *FakeZookeeper) ChildrenApply(clientID FakeClientID) {
	input := getActionWithType[ChildrenInput](s, clientID, "Children")

	parent := s.findParentNode(input.Path)
	var children []string
	for _, child := range parent.Children {
		children = append(children, child.Name)
	}

	input.Callback(zk.ChildrenResponse{
		Zxid:     s.Zxid,
		Children: children,
	}, nil)
}

func (s *FakeZookeeper) CreateConnError(clientID FakeClientID) {
	input := getActionWithType[CreateInput](s, clientID, "Create")
	input.Callback(zk.CreateResponse{}, zk.ErrConnectionClosed)
	s.appendActions(clientID, RetryInput{})
}

func (s *FakeZookeeper) Retry(clientID FakeClientID) {
	getActionWithType[RetryInput](s, clientID, "Retry")

	sessList := s.Sessions[clientID]
	for _, sess := range sessList {
		sess.Retry()
	}
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

func (s *FakeZookeeper) appendActions(clientID FakeClientID, action any) {
	s.Pending[clientID] = append(s.Pending[clientID], action)
}

func (c *fakeClient) Children(path string, callback func(resp zk.ChildrenResponse, err error)) {
	input := ChildrenInput{
		Path:     path,
		Callback: callback,
	}
	c.store.appendActions(c.clientID, input)
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
	input := CreateInput{
		Path:     path,
		Data:     data,
		Flags:    flags,
		Callback: callback,
	}
	c.store.appendActions(c.clientID, input)
}
