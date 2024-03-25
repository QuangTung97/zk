package curator

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/zk"
)

const client1 FakeClientID = "client01"

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
			sess.Run(func(client Client) {
				client.Create(
					"/workers", []byte("data01"), zk.FlagEphemeral,
					func(resp zk.CreateResponse, err error) {
						createResp = resp
						createErr = err
					},
				)
			})
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
					},
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
			sess.Run(func(client Client) {
				client.Create(
					"/workers", []byte("data01"), zk.FlagEphemeral,
					func(resp zk.CreateResponse, err error) {
						c.addStep("create-resp")
						if err != nil {
							sess.AddRetry(initFn)
						}
					},
				)
			})
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
					},
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
			sess.Run(func(client Client) {
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
		sess.Run(func(client Client) {
			client.Children("/workers", func(resp zk.ChildrenResponse, err error) {
				childrenErr = err
			})
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
		sess.Run(func(client Client) {
			client.Create("/workers/node01", nil, zk.FlagEphemeral,
				func(resp zk.CreateResponse, err error) {
					createErr = err
				},
			)
		})
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
		sess.Run(func(client Client) {
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
		})
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
		sess.Run(func(client Client) {
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
		})
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
		sess.Run(func(client Client) {
			client.GetW("/workers", func(resp zk.GetResponse, err error) {
				c.addStep("getw-resp")
				errors = append(errors, err)
			}, func(ev zk.Event) {
				c.addStep("getw-watch")
			})
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
		sess.Run(func(client Client) {
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
		sess.Run(func(client Client) {
			client.Set("/workers", []byte("data01"), 0, func(resp zk.SetResponse, err error) {
				errors = append(errors, err)
			})
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
		sess.Run(func(client Client) {
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
		sess.Run(func(client Client) {
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
		sess.Run(func(client Client) {
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
		})
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
		sess.Run(func(client Client) {
			client.Create("/workers01", []byte("data01"), zk.FlagEphemeral,
				func(resp zk.CreateResponse, err error) {
					errors = append(errors, err)
				},
			)
		})
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
		sess.Run(func(client Client) {
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
		})
	}
	c.startCuratorClient1(initFn)

	c.store.Begin(client1)
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
