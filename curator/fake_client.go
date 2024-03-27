package curator

import (
	"fmt"
	stdpath "path"
	"reflect"
	"slices"

	"github.com/QuangTung97/zk"
)

type FakeSessionState struct {
	SessionID  int64
	HasSession bool
}

type FakeClientID string

type ZNode struct {
	Name     string
	Data     []byte
	Flags    int32
	Children []*ZNode

	SessionID int64

	NextSeq int64

	Stat zk.Stat

	ChildrenWatches []func(ev zk.Event)
	DataWatches     []func(ev zk.Event)
}

type FakeZookeeper struct {
	States   map[FakeClientID]*FakeSessionState
	Sessions map[FakeClientID]SessionRunner
	Clients  map[FakeClientID]Client
	Pending  map[FakeClientID][]any

	Root *ZNode // root znode

	NextSessionID int64

	Zxid int64
}

func NewFakeZookeeper() *FakeZookeeper {
	return &FakeZookeeper{
		States:   map[FakeClientID]*FakeSessionState{},
		Sessions: map[FakeClientID]SessionRunner{},
		Clients:  map[FakeClientID]Client{},
		Pending:  map[FakeClientID][]any{},

		Root: &ZNode{},

		NextSessionID: 500,

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

func (c *fakeClientFactory) Start(runner SessionRunner) {
	c.store.Sessions[c.clientID] = runner
	c.store.Clients[c.clientID] = &fakeClient{
		store:    c.store,
		clientID: c.clientID,
	}
}

func (c *fakeClientFactory) Close() {
}

func (s *FakeZookeeper) Begin(clientID FakeClientID) {
	state := s.States[clientID]
	if state.HasSession {
		panic("can not call Begin on client already had session")
	}
	state.HasSession = true
	s.NextSessionID++
	state.SessionID = s.NextSessionID
	client := s.Clients[clientID]
	runner := s.Sessions[clientID]
	runner.Begin(client)
}

func (s *FakeZookeeper) runAllCallbacksWithConnectionError(clientID FakeClientID) {
	actions := s.Pending[clientID]
	s.Pending[clientID] = nil
	for _, input := range actions {
		switch inputVal := input.(type) {
		case CreateInput:
			inputVal.Callback(zk.CreateResponse{}, zk.ErrConnectionClosed)
		case ChildrenInput:
			inputVal.Callback(zk.ChildrenResponse{}, zk.ErrConnectionClosed)
		case GetInput:
			inputVal.Callback(zk.GetResponse{}, zk.ErrConnectionClosed)
		default:
			panic("unknown input type")
		}
	}
}

func (s *FakeZookeeper) SessionExpired(clientID FakeClientID) {
	state := s.States[clientID]
	if !state.HasSession {
		panic("can not call SessionExpired on client already lost session")
	}
	state.HasSession = false
	sessionID := state.SessionID
	state.SessionID = 0

	s.runAllCallbacksWithConnectionError(clientID)

	s.Zxid++
	s.deleteNodesRecursiveForSessionID(s.Root, "", sessionID)

	runner := s.Sessions[clientID]
	runner.End()
}

func (s *FakeZookeeper) deleteNodesRecursiveForSessionID(parent *ZNode, path string, sessionID int64) {
	var newChildren []*ZNode
	for _, child := range parent.Children {
		childPath := path + "/" + child.Name
		if child.Flags&zk.FlagEphemeral == 0 {
			s.deleteNodesRecursiveForSessionID(child, childPath, sessionID)
			newChildren = append(newChildren, child)
			continue
		}

		if child.SessionID == sessionID {
			s.notifyChildrenWatches(parent, childPath)
			s.notifyDataWatches(child, childPath, zk.EventNodeDeleted)
			continue
		}
		newChildren = append(newChildren, child)
	}
	parent.Children = newChildren
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
	for client, actions := range s.Pending {
		if len(actions) == 0 {
			continue
		}
		fmt.Println("------------------------------------------------")
		fmt.Println("CLIENT:", client)
		for _, input := range actions {
			typeName := reflect.TypeOf(input).Name()
			fmt.Printf("  %s: %+v\n", typeName, input)
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
			if inputVal.Watch {
				values = append(values, "get-w")
			} else {
				values = append(values, "get")
			}
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
	s.createApplyWithErr(clientID, nil)
}

func (s *FakeZookeeper) CreateApplyError(clientID FakeClientID) {
	s.createApplyWithErr(clientID, zk.ErrConnectionClosed)
}

func (s *FakeZookeeper) createApplyWithErr(clientID FakeClientID, err error) {
	input := s.CreateCall(clientID)
	parent := s.findNode(stdpath.Dir(input.Path))
	if parent == nil {
		input.Callback(zk.CreateResponse{}, zk.ErrNoNode)
		return
	}

	nodeName := stdpath.Base(input.Path)
	if input.Flags&zk.FlagSequence != 0 {
		nodeName = nodeName + fmt.Sprintf("%010d", parent.NextSeq)
		parent.NextSeq++
	} else {
		for _, child := range parent.Children {
			if child.Name == nodeName {
				input.Callback(zk.CreateResponse{}, zk.ErrNodeExists)
				return
			}
		}
	}

	s.Zxid++
	parent.Children = append(parent.Children, &ZNode{
		Name:  nodeName,
		Data:  input.Data,
		Flags: input.Flags,
		Stat: zk.Stat{
			Czxid: s.Zxid,
			Mzxid: s.Zxid,
		},
		SessionID: s.States[clientID].SessionID,
	})

	s.notifyChildrenWatches(parent, input.Path)

	if err != nil {
		input.Callback(zk.CreateResponse{}, err)
		s.ConnError(clientID)
		return
	}

	input.Callback(zk.CreateResponse{
		Zxid: s.Zxid,
		Path: input.Path,
	}, nil)
}

func (s *FakeZookeeper) notifyChildrenWatches(parent *ZNode, path string) {
	for _, w := range parent.ChildrenWatches {
		w(zk.Event{
			Type:  zk.EventNodeChildrenChanged,
			State: 3,
			Path:  stdpath.Dir(path),
		})
	}
	parent.ChildrenWatches = nil
}

func (s *FakeZookeeper) ConnError(clientID FakeClientID) {
	s.runAllCallbacksWithConnectionError(clientID)
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

	if input.Watch {
		node.DataWatches = append(node.DataWatches, input.Watcher)
	}

	input.Callback(zk.GetResponse{
		Zxid: s.Zxid,
		Data: node.Data,
		Stat: node.Stat,
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

	s.notifyDataWatches(node, input.Path, zk.EventNodeDataChanged)

	input.Callback(zk.SetResponse{
		Zxid: s.Zxid,
		Stat: node.Stat,
	}, nil)
}

func (s *FakeZookeeper) notifyDataWatches(node *ZNode, path string, eventType zk.EventType) {
	for _, w := range node.DataWatches {
		w(zk.Event{
			Type:  eventType,
			State: 3,
			Path:  path,
		})
	}
	node.DataWatches = nil
}

func (s *FakeZookeeper) Retry(clientID FakeClientID) {
	getActionWithType[RetryInput](s, clientID, "Retry")

	runner := s.Sessions[clientID]
	runner.Retry()
}

type fakeClient struct {
	store    *FakeZookeeper
	clientID FakeClientID
}

func (c *fakeClient) Get(path string, callback func(resp zk.GetResponse, err error)) {
	validatePath(path)
	input := GetInput{
		Path:     path,
		Callback: callback,
	}
	c.store.appendActions(c.clientID, input)
}

func (c *fakeClient) GetW(path string,
	callback func(resp zk.GetResponse, err error),
	watcher func(ev zk.Event),
) {
	validatePath(path)
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
	validatePath(path)
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
	validatePath(path)
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
	validatePath(path)
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
	validatePath(path)
	input := SetInput{
		Path:     path,
		Data:     data,
		Version:  version,
		Callback: callback,
	}
	c.store.appendActions(c.clientID, input)
}

func validatePath(path string) {
	if err := zk.ValidatePath(path, false); err != nil {
		panic(err)
	}
}
