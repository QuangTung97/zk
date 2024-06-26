package curator

import (
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/zk"
)

const client1 FakeClientID = "client01"
const client2 FakeClientID = "client02"
const client3 FakeClientID = "client03"

type fakeClientTest struct {
	store *FakeZookeeper
	steps []string
}

func (c *fakeClientTest) addStep(s string) {
	c.steps = append(c.steps, s)
}

func newFakeClientTest() *fakeClientTest {
	store := NewFakeZookeeper()
	return &fakeClientTest{
		store: store,
		steps: []string{},
	}
}

func (c *fakeClientTest) startCuratorClient1(initFn func(sess *Session)) {
	c1 := New(initFn)
	f1 := NewFakeClientFactory(c.store, client1)
	f1.Start(c1)
}

func TestFakeClient_CreateUntilSuccess(t *testing.T) {
	t.Run("success on first try", func(t *testing.T) {
		c := newFakeClientTest()

		var createResp zk.CreateResponse
		var createErr error

		initFn := func(sess *Session) {
			sess.GetClient().Create(
				"/workers", []byte("data01"), zk.FlagEphemeral,
				func(resp zk.CreateResponse, err error) {
					createResp = resp
					createErr = err
				},
			)
		}
		c.startCuratorClient1(initFn)

		c.store.Begin(client1)

		assert.Equal(t, []string{"create"}, c.store.PendingCalls(client1))

		// Create Success
		c.store.CreateApply(client1)

		assert.Equal(t, &ZNode{
			Children: []*ZNode{
				{
					Name:  "workers",
					Data:  []byte("data01"),
					Flags: zk.FlagEphemeral,
					Stat: zk.Stat{
						Czxid: 101,
						Mzxid: 101,
					},
					SessionID: 501,
				},
			},
		}, c.store.Root)

		assert.Equal(t, zk.CreateResponse{
			Zxid: 101,
			Path: "/workers",
		}, createResp)
		assert.Equal(t, nil, createErr)

		assert.Equal(t, []string{}, c.store.PendingCalls(client1))
	})

	t.Run("success on second try", func(t *testing.T) {
		c := newFakeClientTest()

		var initFn func(sess *Session)
		initFn = func(sess *Session) {
			sess.GetClient().Create(
				"/workers", []byte("data01"), zk.FlagEphemeral,
				func(resp zk.CreateResponse, err error) {
					c.addStep("create-resp")
					if err != nil {
						sess.AddRetry(initFn)
					}
				},
			)
		}
		c.startCuratorClient1(initFn)

		c.store.Begin(client1)
		assert.Equal(t, []string{"create"}, c.store.PendingCalls(client1))

		c.store.ConnError(client1)
		assert.Equal(t, []string{"retry"}, c.store.PendingCalls(client1))

		c.store.Retry(client1)
		assert.Equal(t, []string{"create"}, c.store.PendingCalls(client1))

		c.store.CreateApply(client1)
		assert.Equal(t, []string{}, c.store.PendingCalls(client1))

		assert.Equal(t, &ZNode{
			Children: []*ZNode{
				{
					Name:  "workers",
					Data:  []byte("data01"),
					Flags: zk.FlagEphemeral,
					Stat: zk.Stat{
						Czxid: 101,
						Mzxid: 101,
					},
					SessionID: 501,
				},
			},
		}, c.store.Root)

		assert.Equal(t, []string{
			"create-resp",
			"create-resp",
		}, c.steps)
	})
}

func TestFakeClient_CreateThenListChildren(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		c := newFakeClientTest()

		var childrenResp zk.ChildrenResponse
		initFn := func(sess *Session) {
			client := sess.GetClient()
			client.Create(
				"/workers01", []byte("data01"), zk.FlagEphemeral,
				func(resp zk.CreateResponse, err error) {},
			)
			client.Create(
				"/workers02", []byte("data01"), zk.FlagEphemeral,
				func(resp zk.CreateResponse, err error) {},
			)
			client.Children("/", func(resp zk.ChildrenResponse, err error) {
				childrenResp = resp
			})
		}
		c.startCuratorClient1(initFn)

		c.store.Begin(client1)
		assert.Equal(t, []string{"create", "create", "children"}, c.store.PendingCalls(client1))

		c.store.CreateApply(client1)
		c.store.CreateApply(client1)

		assert.Equal(t, []string{"children"}, c.store.PendingCalls(client1))

		c.store.ChildrenApply(client1)
		assert.Equal(t, []string{}, c.store.PendingCalls(client1))

		assert.Equal(t, zk.ChildrenResponse{
			Zxid: 102,
			Children: []string{
				"workers01",
				"workers02",
			},
		}, childrenResp)
	})
}

func TestFakeClient_ListChildren_Not_Found_Parent(t *testing.T) {
	c := newFakeClientTest()

	var childrenErr error
	initFn := func(sess *Session) {
		sess.GetClient().Children("/workers", func(resp zk.ChildrenResponse, err error) {
			childrenErr = err
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"children"}, c.store.PendingCalls(client1))

	c.store.ChildrenApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, zk.ErrNoNode, childrenErr)
}

func TestFakeClient_Create_Parent_Not_Found(t *testing.T) {
	c := newFakeClientTest()

	var createErr error
	initFn := func(sess *Session) {
		sess.GetClient().Create("/workers/node01", nil, zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				createErr = err
			},
		)
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"create"}, c.store.PendingCalls(client1))

	c.store.CreateApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, zk.ErrNoNode, createErr)
}

func TestFakeClient_Create_Duplicated(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.Create("/workers", nil, zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)
		client.Create("/workers", nil, zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"create", "create"}, c.store.PendingCalls(client1))

	c.store.CreateApply(client1)

	c.store.CreateApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{
		nil,
		zk.ErrNodeExists,
	}, errors)
}

func TestFakeClient_ListChildren_With_Watch(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	var watchEvent zk.Event
	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.ChildrenW("/", func(resp zk.ChildrenResponse, err error) {
			errors = append(errors, err)
		}, func(ev zk.Event) {
			watchEvent = ev
		})

		client.Create("/workers", nil, zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"children-w", "create"}, c.store.PendingCalls(client1))

	c.store.ChildrenApply(client1)
	c.store.CreateApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{
		nil,
		nil,
	}, errors)

	assert.Equal(t, zk.Event{
		Type:  zk.EventNodeChildrenChanged,
		State: 3,
		Path:  "/",
	}, watchEvent)
}

func TestFakeClient_GetW_Not_Found(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.GetW("/workers", func(resp zk.GetResponse, err error) {
			c.addStep("getw-resp")
			errors = append(errors, err)
		}, func(ev zk.Event) {
			c.addStep("getw-watch")
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"get-w"}, c.store.PendingCalls(client1))

	c.store.GetApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{
		zk.ErrNoNode,
	}, errors)
	assert.Equal(t, []string{"getw-resp"}, c.steps)
}

func TestFakeClient_GetW_Found(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	var getResp zk.GetResponse
	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.Create("/workers", []byte("data01"), zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				c.addStep("create-resp")
				errors = append(errors, err)
			},
		)

		client.GetW("/workers", func(resp zk.GetResponse, err error) {
			c.addStep("getw-resp")
			errors = append(errors, err)
			getResp = resp
		}, func(ev zk.Event) {
			c.addStep("getw-watch")
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"create", "get-w"}, c.store.PendingCalls(client1))

	c.store.CreateApply(client1)
	c.store.GetApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, int64(101), getResp.Zxid)
	assert.Equal(t, "data01", string(getResp.Data))

	assert.Equal(t, []error{
		nil,
		nil,
	}, errors)
	assert.Equal(t, []string{
		"create-resp",
		"getw-resp",
	}, c.steps)
}

func TestFakeClient_Set_Not_Found(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	initFn := func(sess *Session) {
		sess.GetClient().Set("/workers", []byte("data01"), 0, func(resp zk.SetResponse, err error) {
			errors = append(errors, err)
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"set"}, c.store.PendingCalls(client1))

	c.store.SetApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{
		zk.ErrNoNode,
	}, errors)
}

func TestFakeClient_Create_Then_Set(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	var respList []zk.SetResponse
	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.Create("/workers", []byte("data01"), zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)

		client.Set("/workers", []byte("data02"), 0, func(resp zk.SetResponse, err error) {
			errors = append(errors, err)
			respList = append(respList, resp)
		})
		client.Set("/workers", []byte("data03"), 0, func(resp zk.SetResponse, err error) {
			errors = append(errors, err)
			respList = append(respList, resp)
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"create", "set", "set"}, c.store.PendingCalls(client1))

	c.store.CreateApply(client1)
	c.store.SetApply(client1)
	c.store.SetApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{
		nil,
		nil,
		zk.ErrBadVersion,
	}, errors)
	assert.Equal(t, []zk.SetResponse{
		{
			Zxid: 102,
			Stat: zk.Stat{
				Czxid:   101,
				Mzxid:   102,
				Version: 1,
			},
		},
		{},
	}, respList)
}

func TestFakeClient_Create_Then_Getw_Then_Set(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	var getResp zk.GetResponse
	var watchEvent zk.Event
	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.Create("/workers", []byte("data01"), zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
			},
		)

		client.GetW("/workers", func(resp zk.GetResponse, err error) {
			getResp = resp
			errors = append(errors, err)
			c.addStep("get-resp")
		}, func(ev zk.Event) {
			watchEvent = ev
			c.addStep("get-watch")
		})

		client.Set("/workers", []byte("data02"), 0, func(resp zk.SetResponse, err error) {
			c.addStep("set-resp")
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"create", "get-w", "set"}, c.store.PendingCalls(client1))

	c.store.CreateApply(client1)
	c.store.GetApply(client1)
	c.store.SetApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{
		nil,
	}, errors)
	assert.Equal(t, zk.GetResponse{
		Zxid: 101,
		Data: []byte("data01"),
		Stat: zk.Stat{
			Czxid: 101,
			Mzxid: 101,
		},
	}, getResp)
	assert.Equal(t, zk.Event{
		Type:  zk.EventNodeDataChanged,
		State: 3,
		Path:  "/workers",
	}, watchEvent)

	assert.Equal(t, []string{
		"get-resp",
		"get-watch",
		"set-resp",
	}, c.steps)
}

func TestFakeClient_Create_Then_Session_Expired(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.Create("/workers01", []byte("data01"), zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)
		client.Create("/workers02", []byte("data01"), zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"create", "create"}, c.store.PendingCalls(client1))

	c.store.SessionExpired(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{
		zk.ErrConnectionClosed,
		zk.ErrConnectionClosed,
	}, errors)
}

func TestFakeClient_Begin_Multiple_Times_Panics(t *testing.T) {
	c := newFakeClientTest()

	initFn := func(sess *Session) {}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.PanicsWithValue(t, "can not call Begin on client already had session", func() {
		c.store.Begin(client1)
	})
}

func TestFakeClient_Create_Then_Session_Expired__Then_New_Session_Established(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	initFn := func(sess *Session) {
		sess.GetClient().Create("/workers01", []byte("data01"), zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"create"}, c.store.PendingCalls(client1))

	c.store.SessionExpired(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	c.store.Begin(client1)
	assert.Equal(t, []string{"create"}, c.store.PendingCalls(client1))

	c.store.CreateApply(client1)

	assert.Equal(t, []error{
		zk.ErrConnectionClosed,
		nil,
	}, errors)
}

func TestFakeClient_Create_With_Sequence(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	var childrenResp zk.ChildrenResponse
	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.Create("/node01-", []byte("data01"), zk.FlagEphemeral|zk.FlagSequence,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)
		client.Create("/node01-", []byte("data02"), zk.FlagEphemeral|zk.FlagSequence,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)
		client.Children("/", func(resp zk.ChildrenResponse, err error) {
			childrenResp = resp
			errors = append(errors, err)
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)

	c.store.PrintPendingCalls()

	assert.Equal(t, []string{"create", "create", "children"}, c.store.PendingCalls(client1))

	c.store.CreateApply(client1)
	c.store.CreateApply(client1)
	c.store.ChildrenApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{nil, nil, nil}, errors)
	assert.Equal(t, zk.ChildrenResponse{
		Zxid: 102,
		Children: []string{
			"node01-0000000000",
			"node01-0000000001",
		},
	}, childrenResp)
}

func TestFakeClient_Create_With_Ephemeral__Then_Session_Expired(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	initFn := func(sess *Session) {
		sess.GetClient().Create("/node01", []byte("data01"), zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"create"}, c.store.PendingCalls(client1))

	c.store.CreateApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	c.store.SessionExpired(client1)

	// Create Client 2
	var childrenResp zk.ChildrenResponse
	NewFakeClientFactory(c.store, client2).Start(New(func(sess *Session) {
		sess.GetClient().Children("/", func(resp zk.ChildrenResponse, err error) {
			childrenResp = resp
		})
	}))
	c.store.Begin(client2)
	c.store.ChildrenApply(client2)
	assert.Equal(t, []string{}, c.store.PendingCalls(client2))

	assert.Equal(t, zk.ChildrenResponse{
		Zxid:     102,
		Children: nil,
	}, childrenResp)
}

func TestFakeClient_Create_With_Ephemeral_On_Two_Clients(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	var childrenResp zk.ChildrenResponse

	NewFakeClientFactory(c.store, client1).Start(New(func(sess *Session) {
		sess.GetClient().Create("/node01", nil, zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)
	}))

	NewFakeClientFactory(c.store, client2).Start(New(func(sess *Session) {
		client := sess.GetClient()
		client.Create("/node02", nil, zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)
		client.Children("/", func(resp zk.ChildrenResponse, err error) {
			childrenResp = resp
			errors = append(errors, err)
		})
	}))

	c.store.Begin(client1)
	c.store.Begin(client2)

	c.store.CreateApply(client1)
	c.store.CreateApply(client2)

	c.store.SessionExpired(client1)

	c.store.ChildrenApply(client2)

	assert.Equal(t, []error{nil, nil, nil}, errors)
	assert.Equal(t, zk.ChildrenResponse{
		Zxid:     103,
		Children: []string{"node02"},
	}, childrenResp)
}

func TestFakeClient_Session_Expired_Another_Client_Watch_Children_And_Watch_Data(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	var childrenResp zk.ChildrenResponse
	var getResp zk.GetResponse
	var watchEvents []zk.Event

	NewFakeClientFactory(c.store, client1).Start(New(func(sess *Session) {
		sess.GetClient().Create("/node01", []byte("data01"), zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)
	}))

	NewFakeClientFactory(c.store, client2).Start(New(func(sess *Session) {
		client := sess.GetClient()
		client.ChildrenW("/", func(resp zk.ChildrenResponse, err error) {
			childrenResp = resp
			errors = append(errors, err)
			c.addStep("children-resp")
		}, func(ev zk.Event) {
			watchEvents = append(watchEvents, ev)
			c.addStep("children-watch")
		})
		client.GetW("/node01", func(resp zk.GetResponse, err error) {
			errors = append(errors, err)
			getResp = resp
			c.addStep("get-resp")
		}, func(ev zk.Event) {
			watchEvents = append(watchEvents, ev)
			c.addStep("get-watch")
		})
	}))

	c.store.Begin(client1)
	c.store.Begin(client2)

	c.store.CreateApply(client1)

	c.store.ChildrenApply(client2)
	c.store.GetApply(client2)

	assert.Equal(t, []string{}, c.store.PendingCalls(client1))
	assert.Equal(t, []string{}, c.store.PendingCalls(client2))

	c.store.SessionExpired(client1)

	assert.Equal(t, []error{nil, nil, nil}, errors)
	assert.Equal(t, []string{
		"children-resp",
		"get-resp",
		"children-watch",
		"get-watch",
	}, c.steps)

	assert.Equal(t, zk.ChildrenResponse{
		Zxid:     101,
		Children: []string{"node01"},
	}, childrenResp)

	assert.Equal(t, zk.GetResponse{
		Zxid: 101,
		Data: []byte("data01"),
		Stat: zk.Stat{
			Czxid: 101,
			Mzxid: 101,
		},
	}, getResp)

	assert.Equal(t, []zk.Event{
		{
			Type:  zk.EventNodeChildrenChanged,
			State: 3,
			Path:  "/",
		},
		{
			Type:  zk.EventNodeDeleted,
			State: 3,
			Path:  "/node01",
		},
	}, watchEvents)
}

func TestFakeClient_Get_With_Not_Found_And_Found(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	var getRespList []zk.GetResponse

	const pathValue = "/node01"

	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.Get(pathValue, func(resp zk.GetResponse, err error) {
			errors = append(errors, err)
			getRespList = append(getRespList, resp)
		})

		client.Create(pathValue, []byte("data01"), zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				errors = append(errors, err)
			},
		)

		client.Get(pathValue, func(resp zk.GetResponse, err error) {
			errors = append(errors, err)
			getRespList = append(getRespList, resp)
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"get", "create", "get"}, c.store.PendingCalls(client1))

	c.store.GetApply(client1)
	c.store.CreateApply(client1)
	c.store.GetApply(client1)

	assert.Equal(t, []error{
		zk.ErrNoNode, nil, nil,
	}, errors)
	assert.Equal(t, []zk.GetResponse{
		{},
		{
			Zxid: 101,
			Data: []byte("data01"),
			Stat: zk.Stat{
				Czxid: 101,
				Mzxid: 101,
			},
		},
	}, getRespList)
}

func TestFakeClient_Set_Validate_Error(t *testing.T) {
	c := newFakeClientTest()

	initFn := func(sess *Session) {
		sess.GetClient().Set("/sample/", nil, 0, func(resp zk.SetResponse, err error) {
		})
	}
	c.startCuratorClient1(initFn)

	assert.PanicsWithValue(t, zk.ErrInvalidPath, func() {
		c.store.Begin(client1)
	})
}

func TestFakeClient_Print_Data(t *testing.T) {
	c := newFakeClientTest()

	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.Create("/node01", nil, 0, func(resp zk.CreateResponse, err error) {})
		client.Create("/node02", nil, 0, func(resp zk.CreateResponse, err error) {})
		client.Create("/node03", []byte("data01"), 0, func(resp zk.CreateResponse, err error) {})
		client.Create("/node01/child01", []byte("data02"), 0, func(resp zk.CreateResponse, err error) {})
		client.Create("/node01/child02", nil, 0, func(resp zk.CreateResponse, err error) {})
		client.Create("/node03/child03", []byte("data03"), zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {},
		)
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)

	c.store.CreateApply(client1)
	c.store.CreateApply(client1)
	c.store.CreateApply(client1)
	c.store.CreateApply(client1)
	c.store.CreateApply(client1)
	c.store.CreateApply(client1)

	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	c.store.PrintData()
}

func TestComputePathNodes(t *testing.T) {
	assert.Equal(t, []string{
		"/", "workers",
	}, computePathNodes("/workers"))

	assert.Equal(t, []string{
		"/",
	}, computePathNodes("/"))

	assert.Equal(t, []string{
		"/", "data", "tmp01",
	}, computePathNodes("/data/tmp01"))
}

func TestFakeClient_Delete_Not_Found(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	initFn := func(sess *Session) {
		sess.GetClient().Delete("/workers", 0, func(resp zk.DeleteResponse, err error) {
			errors = append(errors, err)
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"delete"}, c.store.PendingCalls(client1))

	c.store.DeleteApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{
		zk.ErrNoNode,
	}, errors)
}

func TestFakeClient_Delete_After_Create(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	var deleteResp zk.DeleteResponse
	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.Create("/workers", nil, 0, func(resp zk.CreateResponse, err error) {
			errors = append(errors, err)
		})
		client.Delete("/workers", 0, func(resp zk.DeleteResponse, err error) {
			deleteResp = resp
			errors = append(errors, err)
		})
		client.Get("/workers", func(resp zk.GetResponse, err error) {
			errors = append(errors, err)
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"create", "delete", "get"}, c.store.PendingCalls(client1))

	c.store.CreateApply(client1)
	c.store.DeleteApply(client1)
	c.store.GetApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{
		nil,
		nil,
		zk.ErrNoNode,
	}, errors)

	assert.Equal(t, zk.DeleteResponse{
		Zxid: 102,
	}, deleteResp)
}

func TestFakeClient_Delete_Conflict_Version(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.Create("/workers", nil, 0, func(resp zk.CreateResponse, err error) {
			errors = append(errors, err)
		})
		client.Set("/workers", []byte("new-data"), 0, func(resp zk.SetResponse, err error) {
			errors = append(errors, err)
		})
		client.Delete("/workers", 0, func(resp zk.DeleteResponse, err error) {
			errors = append(errors, err)
		})
		client.Get("/workers", func(resp zk.GetResponse, err error) {
			errors = append(errors, err)
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"create", "set", "delete", "get"}, c.store.PendingCalls(client1))

	c.store.CreateApply(client1)
	c.store.SetApply(client1)
	c.store.DeleteApply(client1)
	c.store.GetApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{
		nil,
		nil,
		zk.ErrBadVersion,
		nil,
	}, errors)
}

func TestFakeClient_Delete_Data_Deleted_Watch(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	var watchEvent zk.Event
	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.Create("/workers", nil, 0, func(resp zk.CreateResponse, err error) {
			errors = append(errors, err)
		})
		client.GetW("/workers", func(resp zk.GetResponse, err error) {
			errors = append(errors, err)
		}, func(ev zk.Event) {
			c.addStep("get-watch")
			watchEvent = ev
		})
		client.Delete("/workers", 0, func(resp zk.DeleteResponse, err error) {
			c.addStep("delete-resp")
			errors = append(errors, err)
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"create", "get-w", "delete"}, c.store.PendingCalls(client1))

	c.store.CreateApply(client1)
	c.store.GetApply(client1)
	c.store.DeleteApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{
		nil,
		nil,
		nil,
	}, errors)

	assert.Equal(t, []string{
		"get-watch",
		"delete-resp",
	}, c.steps)

	assert.Equal(t, zk.Event{
		Type:  zk.EventNodeDeleted,
		State: 3,
		Path:  "/workers",
	}, watchEvent)
}

func TestFakeClient_Delete_Children_Watch(t *testing.T) {
	c := newFakeClientTest()

	var errors []error
	var watchEvent zk.Event
	initFn := func(sess *Session) {
		client := sess.GetClient()
		client.Create("/workers", nil, 0, func(resp zk.CreateResponse, err error) {
			errors = append(errors, err)
		})
		client.ChildrenW("/", func(resp zk.ChildrenResponse, err error) {
			errors = append(errors, err)
		}, func(ev zk.Event) {
			c.addStep("children-watch")
			watchEvent = ev
		})
		client.Delete("/workers", 0, func(resp zk.DeleteResponse, err error) {
			c.addStep("delete-resp")
			errors = append(errors, err)
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
	assert.Equal(t, []string{"create", "children-w", "delete"}, c.store.PendingCalls(client1))

	c.store.CreateApply(client1)
	c.store.ChildrenApply(client1)
	c.store.DeleteApply(client1)
	assert.Equal(t, []string{}, c.store.PendingCalls(client1))

	assert.Equal(t, []error{
		nil,
		nil,
		nil,
	}, errors)

	assert.Equal(t, []string{
		"children-watch",
		"delete-resp",
	}, c.steps)

	assert.Equal(t, zk.Event{
		Type:  zk.EventNodeChildrenChanged,
		State: 3,
		Path:  "/",
	}, watchEvent)
}

func TestFakeClient_Should_Not_Have_Any_Action_After_Conn_Error(t *testing.T) {
	c := newFakeClientTest()

	c.startCuratorClient1(func(sess *Session) {
		sess.GetClient().Get("/workers", func(resp zk.GetResponse, err error) {
			if err != nil {
				if stderrors.Is(err, zk.ErrConnectionClosed) {
					sess.GetClient().Get("/another", func(resp zk.GetResponse, err error) {})
					return
				}
				panic(err)
			}
		})
	})

	c.store.Begin(client1)
	assert.Equal(t, []string{"get"}, c.store.PendingCalls(client1))

	assert.PanicsWithValue(t, "can not add any more actions after connection error", func() {
		c.store.ConnError(client1)
	})
}

func TestFakeClient_Should_Not_Recv_Watch_After_Connection_Error(t *testing.T) {
	c := newFakeClientTest()

	NewFakeClientFactory(c.store, client2).Start(New(func(sess *Session) {
		sess.GetClient().GetW("/worker", func(resp zk.GetResponse, err error) {
			c.addStep("get-resp")
		}, func(ev zk.Event) {
			c.addStep("get-watch")
		})
	}))

	c.startCuratorClient1(func(sess *Session) {
		sess.GetClient().Create("/worker", nil, zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {
		})
	})

	c.store.Begin(client1)
	c.store.Begin(client2)

	c.store.CreateApply(client1)

	c.store.GetApply(client2)
	c.store.ConnError(client2)

	c.store.SessionExpired(client1)

	assert.Equal(t, []string{
		"get-resp",
	}, c.steps)

	c.store.Retry(client2)

	assert.Equal(t, []string{
		"get-resp",
		"get-watch",
	}, c.steps)
}

func TestFakeClient_Should_Not_Recv_Watch_After_Connection_Error_For_ChildrenW(t *testing.T) {
	c := newFakeClientTest()

	NewFakeClientFactory(c.store, client2).Start(New(func(sess *Session) {
		sess.GetClient().ChildrenW("/", func(resp zk.ChildrenResponse, err error) {
			c.addStep("children-resp")
		}, func(ev zk.Event) {
			c.addStep("children-watch")
		})
	}))

	c.startCuratorClient1(func(sess *Session) {
		sess.GetClient().Create("/worker", nil, zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {
		})
	})

	c.store.Begin(client1)
	c.store.Begin(client2)

	c.store.ChildrenApply(client2)
	c.store.ConnError(client2)

	c.store.CreateApply(client1)

	assert.Equal(t, []string{
		"children-resp",
	}, c.steps)

	c.store.Retry(client2)

	assert.Equal(t, []string{
		"children-resp",
		"children-watch",
	}, c.steps)
}

func TestFakeClient_Should_Not_Recv_Watch_After_Conn_Error_And_Expired_For_ChildrenW(t *testing.T) {
	c := newFakeClientTest()

	NewFakeClientFactory(c.store, client2).Start(New(func(sess *Session) {
		sess.GetClient().ChildrenW("/", func(resp zk.ChildrenResponse, err error) {
			c.addStep("children-resp")
		}, func(ev zk.Event) {
			c.addStep("children-watch")
		})
	}))

	c.startCuratorClient1(func(sess *Session) {
		sess.GetClient().Create("/worker", nil, zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {
		})
	})

	c.store.Begin(client1)
	c.store.Begin(client2)

	c.store.ChildrenApply(client2)
	c.store.ConnError(client2)

	c.store.CreateApply(client1)

	assert.Equal(t, []string{
		"children-resp",
	}, c.steps)

	c.store.SessionExpired(client2)
	c.store.Begin(client2)
	c.store.ConnError(client2)
	c.store.Retry(client2)

	assert.Equal(t, []string{
		"children-resp",
		"children-resp",
	}, c.steps)
}

func TestFakeClient_Should_Not_Recv_Watch_After_Expired_For_ChildrenW(t *testing.T) {
	c := newFakeClientTest()

	NewFakeClientFactory(c.store, client2).Start(New(func(sess *Session) {
		sess.GetClient().ChildrenW("/", func(resp zk.ChildrenResponse, err error) {
			c.addStep("children-resp")
		}, func(ev zk.Event) {
			c.addStep("children-watch")
		})
	}))

	c.startCuratorClient1(func(sess *Session) {
		sess.GetClient().Create("/worker", nil, zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {})
	})

	c.store.Begin(client1)
	c.store.Begin(client2)

	c.store.ChildrenApply(client2)

	assert.Equal(t, []string{
		"children-resp",
	}, c.steps)

	c.store.SessionExpired(client2)

	c.store.CreateApply(client1)

	assert.Equal(t, []string{
		"children-resp",
	}, c.steps)
}

func TestFakeClient_Retry_Happens_Before_Watch_Handlers(t *testing.T) {
	c := newFakeClientTest()

	var getFunc func(sess *Session)
	getFunc = func(sess *Session) {
		c.addStep("get-req")
		sess.GetClient().Get("/hello", func(resp zk.GetResponse, err error) {
			c.addStep("get-resp")
			if stderrors.Is(err, zk.ErrConnectionClosed) {
				sess.AddRetry(getFunc)
				return
			}
			if err != nil {
				panic(err)
			}
		})
	}

	NewFakeClientFactory(c.store, client2).Start(New(func(sess *Session) {
		sess.GetClient().ChildrenW("/", func(resp zk.ChildrenResponse, err error) {
			c.addStep("children-resp")
		}, func(ev zk.Event) {
			c.addStep("children-watch")
		})

		getFunc(sess)
	}))

	c.startCuratorClient1(func(sess *Session) {
		sess.GetClient().Create("/worker", nil, zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {
		})
	})

	c.store.Begin(client1)
	c.store.Begin(client2)

	c.store.ChildrenApply(client2)
	c.store.ConnError(client2)

	c.store.CreateApply(client1)
	c.store.Retry(client2)

	assert.Equal(t, []string{
		"get-req",
		"children-resp",
		"get-resp",
		"get-req",
		"children-watch",
	}, c.steps)
}

func TestFakeClient_Create_With_Error(t *testing.T) {
	c := newFakeClientTest()

	var errors []error

	var getResp zk.GetResponse
	NewFakeClientFactory(c.store, client2).Start(New(func(sess *Session) {
		sess.GetClient().Get("/worker", func(resp zk.GetResponse, err error) {
			c.addStep("get-resp02")
			getResp = resp
			errors = append(errors, err)
		})
	}))

	c.startCuratorClient1(func(sess *Session) {
		client := sess.GetClient()
		client.Create("/worker", []byte("data01"), zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {
			c.addStep("create-resp01")
			errors = append(errors, err)
		})
		client.Create("/another", nil, zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {
			c.addStep("create-resp02")
			errors = append(errors, err)
		})
	})

	c.store.Begin(client1)
	c.store.Begin(client2)

	c.store.CreateApplyError(client1)
	c.store.GetApply(client2)

	c.store.Retry(client1) // do nothing

	assert.Equal(t, "data01", string(getResp.Data))

	assert.Equal(t, []string{
		"create-resp01",
		"create-resp02",
		"get-resp02",
	}, c.steps)
	assert.Equal(t, []error{
		zk.ErrConnectionClosed,
		zk.ErrConnectionClosed,
		nil,
	}, errors)
}

func TestFakeClient_Conn_Error_Multi_Times(t *testing.T) {
	c := newFakeClientTest()

	c.startCuratorClient1(func(sess *Session) {
		sess.GetClient().Create("/worker", []byte("data01"), zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {
		})
	})

	c.store.Begin(client1)

	c.store.ConnError(client1)

	assert.PanicsWithValue(t, "Can NOT call ConnError multiple times in a row", func() {
		c.store.ConnError(client1)
	})
}

func TestFakeClient_Create_Child_of_Ephemeral_Error(t *testing.T) {
	c := newFakeClientTest()

	var errors []error

	c.startCuratorClient1(func(sess *Session) {
		client := sess.GetClient()
		client.Create("/worker", []byte("data01"), zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {
			errors = append(errors, err)
		})
		client.Create("/worker/node01", []byte("data02"), zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {
			errors = append(errors, err)
		})
	})

	c.store.Begin(client1)
	c.store.CreateApply(client1)
	c.store.CreateApply(client1)

	c.store.PrintData()

	assert.Equal(t, []error{
		nil,
		zk.ErrNoChildrenForEphemerals,
	}, errors)
}

func TestFakeClient__Set_Error(t *testing.T) {
	c := newFakeClientTest()

	var errors []error

	c.startCuratorClient1(func(sess *Session) {
		client := sess.GetClient()
		client.Create("/worker", []byte("data01"), zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {
			c.addStep("create-resp")
			errors = append(errors, err)
		})
		client.Set("/worker", []byte("data02"), 0, func(resp zk.SetResponse, err error) {
			c.addStep("set-resp")
			errors = append(errors, err)
		})
	})

	c.store.Begin(client1)
	c.store.CreateApply(client1)
	c.store.SetApplyError(client1)

	c.store.Retry(client1)

	assert.Equal(t, []error{
		nil,
		zk.ErrConnectionClosed,
	}, errors)
	assert.Equal(t, []string{
		"create-resp",
		"set-resp",
	}, c.steps)

	node := c.store.Root.Children[0]
	assert.Equal(t, "worker", node.Name)
	assert.Equal(t, "data02", string(node.Data))
}

func TestFakeClient__Delete_Error(t *testing.T) {
	c := newFakeClientTest()

	var errors []error

	c.startCuratorClient1(func(sess *Session) {
		client := sess.GetClient()
		client.Create("/worker", []byte("data01"), zk.FlagEphemeral, func(resp zk.CreateResponse, err error) {
			c.addStep("create-resp")
			errors = append(errors, err)
		})
		client.Delete("/worker", 0, func(resp zk.DeleteResponse, err error) {
			c.addStep("delete-resp")
			errors = append(errors, err)
		})
	})

	c.store.Begin(client1)
	c.store.CreateApply(client1)
	c.store.DeleteApplyError(client1)

	assert.Equal(t, []error{
		nil,
		zk.ErrConnectionClosed,
	}, errors)
	assert.Equal(t, []string{
		"create-resp",
		"delete-resp",
	}, c.steps)

	assert.Equal(t, 0, len(c.store.Root.Children))
}

func TestFakeClient__Session_Expired_And_Then_Begin__Not_Keeping_Old_Watch(t *testing.T) {
	c := newFakeClientTest()

	c.startCuratorClient1(func(sess *Session) {
		client := sess.GetClient()
		client.Create("/worker", []byte("data01"), 0, func(resp zk.CreateResponse, err error) {
			c.addStep("create-resp")
		})
		client.GetW("/worker", func(resp zk.GetResponse, err error) {
			c.addStep("getw-resp")
		}, func(ev zk.Event) {
			c.addStep("getw-watch")
		})
	})

	NewFakeClientFactory(c.store, client2).Start(New(func(sess *Session) {
		sess.GetClient().Delete("/worker", 0, func(resp zk.DeleteResponse, err error) {
			c.addStep("delete-resp")
		})
	}))

	c.store.Begin(client1)
	c.store.CreateApply(client1)
	c.store.GetApply(client1)

	c.store.SessionExpired(client1)
	c.store.Begin(client1)

	c.store.Begin(client2)
	c.store.DeleteApply(client2)

	c.store.PrintData()
	c.store.PrintPendingCalls()

	assert.Equal(t, []string{
		"create-resp",
		"getw-resp",
		"delete-resp",
	}, c.steps)
}
