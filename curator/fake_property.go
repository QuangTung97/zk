package curator

import (
	"fmt"
	"math/rand"
	"reflect"
)

// FakeZookeeperTester for property-based testing
type FakeZookeeperTester struct {
	store   *FakeZookeeper
	clients []FakeClientID
	rand    *rand.Rand
}

// NewFakeZookeeperTester ...
func NewFakeZookeeperTester(
	store *FakeZookeeper,
	clients []FakeClientID,
	seed int64,
) *FakeZookeeperTester {
	return &FakeZookeeperTester{
		store:   store,
		clients: clients,
		rand:    rand.New(rand.NewSource(seed)),
	}
}

// Begin ...
func (f *FakeZookeeperTester) Begin() {
	for _, client := range f.clients {
		f.store.Begin(client)
	}
}

func (f *FakeZookeeperTester) getActionableRandomClient() (FakeClientID, bool) {
RetrySelect:
	for {
		clients := make([]FakeClientID, 0, len(f.clients))
		var sessionExpiredClients []FakeClientID
		for _, c := range f.clients {
			if !f.store.States[c].HasSession {
				sessionExpiredClients = append(sessionExpiredClients, c)
				continue
			}

			if len(f.store.Pending[c]) == 0 {
				continue
			}
			clients = append(clients, c)
		}
		if len(clients) == 0 {
			if len(sessionExpiredClients) > 0 {
				index := f.rand.Intn(len(sessionExpiredClients))
				client := sessionExpiredClients[index]
				f.store.Begin(client)
				continue RetrySelect
			}
			return "", false
		}
		index := f.rand.Intn(len(clients))
		return clients[index], true
	}
}

const randMax = 10000

func (f *FakeZookeeperTester) getRandomClient() FakeClientID {
	index := f.rand.Intn(len(f.clients))
	return f.clients[index]
}

func (f *FakeZookeeperTester) doSessionExpired(client FakeClientID) {
	f.store.SessionExpired(client)
}

func (f *FakeZookeeperTester) doConnectionError(client FakeClientID) {
	if f.store.States[client].ConnErr {
		return
	}
	cmds := f.store.Pending[client]
	if len(cmds) == 0 {
		f.store.ConnError(client)
		return
	}
	_, ok := cmds[0].(RetryInput)
	if !ok {
		f.store.ConnError(client)
		return
	}
}

type runConfig struct {
	opsErrorPercent float64
}

func (c runConfig) operationShouldError(randSource *rand.Rand) bool {
	if c.opsErrorPercent == 0 {
		return false
	}
	n := randSource.Intn(randMax)
	end := int(c.opsErrorPercent / 100.0 * randMax)
	if n < end {
		return true
	}
	return false
}

func newRunConfig(options ...RunOption) runConfig {
	conf := runConfig{
		opsErrorPercent: 0.0,
	}
	for _, fn := range options {
		fn(&conf)
	}
	return conf
}

// RunOption ...
type RunOption func(conf *runConfig)

// WithRunOperationErrorPercentage ...
func WithRunOperationErrorPercentage(percent float64) RunOption {
	return func(conf *runConfig) {
		conf.opsErrorPercent = percent
	}
}

// RunSessionExpiredAndConnectionError ...
//
//revive:disable-next-line:cognitive-complexity,cyclomatic
func (f *FakeZookeeperTester) RunSessionExpiredAndConnectionError(
	sessionExpiredPercentage float64,
	connectionErrorPercentage float64,
	numSteps int,
	options ...RunOption,
) int {
	conf := newRunConfig(options...)

	sessionExpiredEnd := int(sessionExpiredPercentage / 100.0 * randMax)
	connectionErrorEnd := int(connectionErrorPercentage / 100.0 * randMax)

	for i := 0; i < numSteps; i++ {
		totalEnd := sessionExpiredEnd + connectionErrorEnd
		x := f.rand.Intn(randMax)
		if x < totalEnd {
			client := f.getRandomClient()
			if !f.store.States[client].HasSession {
				f.store.Begin(client)
				continue
			}
			if x < sessionExpiredEnd {
				f.doSessionExpired(client)
			} else {
				f.doConnectionError(client)
			}
			continue
		}

		client, ok := f.getActionableRandomClient()
		if !ok {
			return i + 1
		}
		genericCmd := f.store.Pending[client][0]
		switch genericCmd.(type) {
		case CreateInput:
			if conf.operationShouldError(f.rand) {
				f.store.CreateApplyError(client)
			} else {
				f.store.CreateApply(client)
			}
		case GetInput:
			f.store.GetApply(client)
		case ChildrenInput:
			f.store.ChildrenApply(client)
		case DeleteInput:
			if conf.operationShouldError(f.rand) {
				f.store.DeleteApplyError(client)
			} else {
				f.store.DeleteApply(client)
			}
		case SetInput:
			if conf.operationShouldError(f.rand) {
				f.store.SetApplyError(client)
			} else {
				f.store.SetApply(client)
			}
		case RetryInput:
			f.store.Retry(client)
		default:
			panic(fmt.Sprintf("unknown command: %s%+v", reflect.TypeOf(genericCmd).String(), genericCmd))
		}
	}

	return numSteps
}
