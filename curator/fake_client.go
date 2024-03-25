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

	Stat zk.Stat

	ChildrenWatches []func(ev zk.Event)
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

func (s *FakeZookeeper) findNode(pathValue string) *ZNode {
	nodes := computePathNodes(pathValue)
	current := s.Root
Outer:
	for i := 1; i < len(nodes); i++ {
		n := nodes[i]
		for _, child := range current.Children {
			if child.Name == n {
				current = child
				continue Outer
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
		switch inputVal := input.(type) {
		case ChildrenInput:
			if inputVal.Watch {
				values = append(values, "children-w")
			} else {
				values = append(values, "children")
			}
		case GetInput:
			values = append(values, "get-w")
		case CreateInput:
			values = append(values, "create")
		case SetInput:
			values = append(values, "set")
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
	parent := s.findNode(stdpath.Dir(input.Path))
	if parent == nil {
		input.Callback(zk.CreateResponse{}, zk.ErrNoNode)
		return
	}

	nodeName := stdpath.Base(input.Path)
	for _, child := range parent.Children {
		if child.Name == nodeName {
			input.Callback(zk.CreateResponse{}, zk.ErrNodeExists)
			return
		}
	}

	s.Zxid++
	parent.Children = append(parent.Children, &ZNode{
		Name:  nodeName,
		Data:  input.Data,
		Flags: input.Flags,
		Stat: zk.Stat{
			Czxid: s.Zxid,
		},
	})

	for _, w := range parent.ChildrenWatches {
		w(zk.Event{
			Type:  zk.EventNodeChildrenChanged,
			State: 3,
			Path:  stdpath.Dir(input.Path),
		})
	}

	input.Callback(zk.CreateResponse{
		Zxid: s.Zxid,
		Path: input.Path,
	}, nil)
}

func (s *FakeZookeeper) CreateConnError(clientID FakeClientID) {
	input := getActionWithType[CreateInput](s, clientID, "Create")
	input.Callback(zk.CreateResponse{}, zk.ErrConnectionClosed)
	s.appendActions(clientID, RetryInput{})
}

func (s *FakeZookeeper) ChildrenApply(clientID FakeClientID) {
	input := getActionWithType[ChildrenInput](s, clientID, "Children")

	parent := s.findNode(input.Path)
	if parent == nil {
		input.Callback(zk.ChildrenResponse{}, zk.ErrNoNode)
		return
	}
	var children []string
	for _, child := range parent.Children {
		children = append(children, child.Name)
	}
	if input.Watch {
		parent.ChildrenWatches = append(parent.ChildrenWatches, input.Watcher)
	}

	input.Callback(zk.ChildrenResponse{
		Zxid:     s.Zxid,
		Children: children,
	}, nil)
}

func (s *FakeZookeeper) GetApply(clientID FakeClientID) {
	input := getActionWithType[GetInput](s, clientID, "Get")

	node := s.findNode(input.Path)
	if node == nil {
		input.Callback(zk.GetResponse{}, zk.ErrNoNode)
		return
	}

	input.Callback(zk.GetResponse{
		Zxid: s.Zxid,
		Data: node.Data,
	}, nil)
}

func (s *FakeZookeeper) SetApply(clientID FakeClientID) {
	input := getActionWithType[SetInput](s, clientID, "Set")

	node := s.findNode(input.Path)
	if node == nil {
		input.Callback(zk.SetResponse{}, zk.ErrNoNode)
		return
	}

	if node.Stat.Version != input.Version {
		input.Callback(zk.SetResponse{}, zk.ErrBadVersion)
		return
	}

	s.Zxid++

	node.Data = input.Data
	node.Stat.Version++
	node.Stat.Mzxid = s.Zxid

	input.Callback(zk.SetResponse{
		Zxid: s.Zxid,
		Stat: node.Stat,
	}, nil)
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
	panic("TODO")
}

func (c *fakeClient) GetW(path string,
	callback func(resp zk.GetResponse, err error),
	watcher func(ev zk.Event),
) {
	input := GetInput{
		Path:     path,
		Callback: callback,
		Watch:    true,
		Watcher:  watcher,
	}
	c.store.appendActions(c.clientID, input)
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
	input := ChildrenInput{
		Path:     path,
		Callback: callback,
		Watch:    true,
		Watcher:  watcher,
	}
	c.store.appendActions(c.clientID, input)
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

func (c *fakeClient) Set(
	path string, data []byte, version int32,
	callback func(resp zk.SetResponse, err error),
) {
	input := SetInput{
		Path:     path,
		Data:     data,
		Version:  version,
		Callback: callback,
	}
	c.store.appendActions(c.clientID, input)
}
