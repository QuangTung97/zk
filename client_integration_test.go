//go:build integration

package zk

import (
	"fmt"
	"log"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClientIntegration_Authenticate(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		t.Skip()

		c, err := newClientInternal([]string{"localhost"}, 12*time.Second)
		assert.Equal(t, nil, err)

		netConn, err := net.Dial("tcp", "localhost:2181")
		assert.Equal(t, nil, err)
		defer func() { _ = netConn.Close() }()

		err = c.authenticate(NewTCPConn(netConn))
		assert.Equal(t, nil, err)

		fmt.Println("SESSION_ID:", c.sessionID)
		fmt.Println("PASS:", c.passwd)
		assert.Equal(t, int32(12_000), c.sessionTimeoutMs)
	})
}

func mustNewClient(_ *testing.T, inputOptions ...Option) *Client {
	ch := make(chan struct{}, 1)

	opts := []Option{
		WithSessionEstablishedCallback(func(c *Client) {
			select {
			case ch <- struct{}{}:
			default:
			}
		}),
	}
	opts = append(opts, inputOptions...)

	c, err := NewClient([]string{"localhost"}, 30*time.Second, opts...)
	if err != nil {
		panic(err)
	}
	<-ch

	clearZKData(c)

	return c
}

func TestClientIntegration_All_Ephemeral(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		c := mustNewClient(t)

		var createResp CreateResponse
		var createErr error
		var calls int

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				calls++
				createResp = resp
				createErr = err
			},
		)
		c.Close()

		assert.Equal(t, nil, createErr)
		assert.Equal(t, 1, calls)

		assert.Greater(t, createResp.Zxid, int64(0))
		createResp.Zxid = 0
		assert.Equal(t, CreateResponse{
			Path: "/workers01",
		}, createResp)

		assert.Greater(t, c.lastZxid, int64(0))
	})

	t.Run("get children", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			nil,
		)

		c.Create(
			"/workers02", []byte("data02"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-resp-02")
			},
		)

		c.Create(
			"/workers03", []byte("data03"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-resp-03")
			},
		)

		var childrenResp ChildrenResponse
		var childrenErr error

		c.Children("/", func(resp ChildrenResponse, err error) {
			steps = append(steps, "children")
			childrenResp = resp
			childrenErr = err
		})

		c.Close()

		assert.Equal(t, []string{
			"create-resp-02",
			"create-resp-03",
			"children",
		}, steps)
		assert.Equal(t, nil, childrenErr)

		assert.Greater(t, childrenResp.Zxid, int64(0))
		childrenResp.Zxid = 0

		slices.Sort(childrenResp.Children)
		assert.Equal(t, ChildrenResponse{
			Children: []string{
				"workers01",
				"workers02",
				"workers03",
				"zookeeper",
			},
		}, childrenResp)
	})

	t.Run("watch children", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var childrenResp ChildrenResponse
		var childrenErr error
		var watchEvent Event

		c.Children("/",
			func(resp ChildrenResponse, err error) {
				steps = append(steps, "children")
				childrenResp = resp
				childrenErr = err
			},
			WithChildrenWatch(func(ev Event) {
				steps = append(steps, "children-watch")
				watchEvent = ev
			}),
		)

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-resp-01")
			},
		)

		c.Create(
			"/workers02", []byte("data02"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-resp-02")
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"children",
			"children-watch",
			"create-resp-01",
			"create-resp-02",
		}, steps)
		assert.Equal(t, nil, childrenErr)

		assert.Greater(t, childrenResp.Zxid, int64(0))
		childrenResp.Zxid = 0

		slices.Sort(childrenResp.Children)
		assert.Equal(t, ChildrenResponse{
			Children: []string{
				"zookeeper",
			},
		}, childrenResp)

		assert.Equal(t, Event{
			Type:  EventNodeChildrenChanged,
			State: 3,
			Path:  "/",
		}, watchEvent)
	})

	t.Run("get data", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-workers01")
			},
		)

		var getResp GetResponse
		var getErr error
		c.Get("/workers01", func(resp GetResponse, err error) {
			steps = append(steps, "get-resp")
			getResp = resp
			getErr = err
		})

		var getErr2 error
		c.Get("/workers02", func(resp GetResponse, err error) {
			steps = append(steps, "get-not-found")
			getErr2 = err
		})

		c.Close()

		assert.Equal(t, []string{
			"create-workers01",
			"get-resp",
			"get-not-found",
		}, steps)

		// check response found
		assert.Equal(t, nil, getErr)

		assert.Greater(t, getResp.Zxid, int64(0))
		getResp.Zxid = 0

		checkStat(t, &getResp.Stat)
		assert.Equal(t, GetResponse{
			Data: []byte("data01"),
		}, getResp)

		// check response not found
		assert.Equal(t, ErrNoNode, getErr2)
	})

	t.Run("get data with watch and then set", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-workers01")
			},
		)

		var getResp GetResponse
		var getErr error
		var getEvent Event
		c.Get(
			"/workers01", func(resp GetResponse, err error) {
				steps = append(steps, "get-resp")
				getResp = resp
				getErr = err
			}, WithGetWatch(func(ev Event) {
				steps = append(steps, "get-event")
				getEvent = ev
			}),
		)

		var setResp SetResponse
		var setErr error
		c.Set("/workers01", []byte("data02"), 0,
			func(resp SetResponse, err error) {
				steps = append(steps, "set-data")
				setResp = resp
				setErr = err
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"create-workers01",
			"get-resp",
			"get-event",
			"set-data",
		}, steps)

		assert.Equal(t, nil, getErr)
		assert.Greater(t, getResp.Zxid, int64(0))
		getResp.Zxid = 0
		checkStat(t, &getResp.Stat)
		assert.Equal(t, GetResponse{
			Data: []byte("data01"),
		}, getResp)

		assert.Equal(t, Event{
			Type:  EventNodeDataChanged,
			State: 3,
			Path:  "/workers01",
		}, getEvent)

		// Check Set Response
		assert.Equal(t, nil, setErr)
		assert.Greater(t, setResp.Zxid, int64(0))
		setResp.Zxid = 0
		checkStat(t, &setResp.Stat)
		assert.Equal(t, SetResponse{}, setResp)
	})

	t.Run("set not found", func(t *testing.T) {
		c := mustNewClient(t)

		var setErr error
		c.Set(
			"/workers01", []byte("data01"), 0,
			func(resp SetResponse, err error) {
				setErr = err
			},
		)

		c.Close()

		assert.Equal(t, ErrNoNode, setErr)
	})

	t.Run("set version conflicted", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-workers01")
			},
		)

		var setErrs []error

		c.Set("/workers01", []byte("data02"), 0,
			func(resp SetResponse, err error) {
				steps = append(steps, "set-data01")
				setErrs = append(setErrs, err)
			},
		)

		c.Set("/workers01", []byte("data03"), 1,
			func(resp SetResponse, err error) {
				steps = append(steps, "set-data02")
				setErrs = append(setErrs, err)
			},
		)

		c.Set("/workers01", []byte("data04"), 1,
			func(resp SetResponse, err error) {
				steps = append(steps, "set-data03")
				setErrs = append(setErrs, err)
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"create-workers01",
			"set-data01",
			"set-data02",
			"set-data03",
		}, steps)

		assert.Equal(t, []error{
			nil,
			nil,
			ErrBadVersion,
		}, setErrs)
	})

	t.Run("get data with watch node deleted by session expired", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-workers01")
			},
		)

		var getEvent Event
		c.Get("/workers01",
			func(resp GetResponse, err error) {
				steps = append(steps, "get-data")
			}, WithGetWatch(func(ev Event) {
				steps = append(steps, "get-event")
				getEvent = ev
			}),
		)

		c.Close()

		assert.Equal(t, []string{
			"create-workers01",
			"get-data",
			"get-event",
		}, steps)

		assert.Equal(t, Event{
			Type:  EventNodeDeleted,
			State: 3,
			Path:  "/workers01",
		}, getEvent)
	})

	t.Run("get children with watch node deleted by session expired", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-workers01")
			},
		)

		var childrenEvent Event
		c.Children("/workers01",
			func(resp ChildrenResponse, err error) {
				steps = append(steps, "children-data")
			}, WithChildrenWatch(func(ev Event) {
				steps = append(steps, "children-event")
				childrenEvent = ev
			}),
		)

		c.Close()

		assert.Equal(t, []string{
			"create-workers01",
			"children-data",
			"children-event",
		}, steps)

		assert.Equal(t, Event{
			Type:  EventNodeDeleted,
			State: 3,
			Path:  "/workers01",
		}, childrenEvent)
	})

	t.Run("children watch root and child node deleted by session expired", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-workers01")
			},
		)

		var childrenEvent Event
		c.Children("/",
			func(resp ChildrenResponse, err error) {
				steps = append(steps, "children-data")
			}, WithChildrenWatch(func(ev Event) {
				steps = append(steps, "children-event")
				childrenEvent = ev
			}),
		)

		c.Close()

		assert.Equal(t, []string{
			"create-workers01",
			"children-data",
			"children-event",
		}, steps)

		assert.Equal(t, Event{
			Type:  EventNodeChildrenChanged,
			State: 3,
			Path:  "/",
		}, childrenEvent)
	})

	t.Run("exists empty", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var existsErr error

		c.Exists("/workers01",
			func(resp ExistsResponse, err error) {
				steps = append(steps, "exists-resp")
				existsErr = err
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"exists-resp",
		}, steps)

		assert.Equal(t, ErrNoNode, existsErr)
	})

	t.Run("exists normal", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-workers01")
			},
		)

		var existsResp ExistsResponse
		var existsErr error

		c.Exists("/workers01",
			func(resp ExistsResponse, err error) {
				steps = append(steps, "exists-resp")
				existsResp = resp
				existsErr = err
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"create-workers01",
			"exists-resp",
		}, steps)

		assert.Equal(t, nil, existsErr)
		assert.Greater(t, existsResp.Zxid, int64(0))
		checkStat(t, &existsResp.Stat)
	})

	t.Run("exists with watch create", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var existsErr error
		var watchEvent Event

		c.Exists("/workers01",
			func(resp ExistsResponse, err error) {
				steps = append(steps, "exists-resp")
				existsErr = err
			},
			WithExistsWatch(func(ev Event) {
				steps = append(steps, "exists-watch")
				watchEvent = ev
			}),
		)

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-workers01")
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"exists-resp",
			"exists-watch",
			"create-workers01",
		}, steps)

		assert.Equal(t, ErrNoNode, existsErr)
		assert.Equal(t, Event{
			Type:  EventNodeCreated,
			State: 3,
			Path:  "/workers01",
		}, watchEvent)
	})

	t.Run("create children of ephemeral", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var errors []error

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create")
				errors = append(errors, err)
			},
		)

		c.Create(
			"/workers01/child", []byte("data02"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-child")
				errors = append(errors, err)
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"create",
			"create-child",
		}, steps)

		assert.Equal(t, []error{
			nil,
			ErrNoChildrenForEphemerals,
		}, errors)
	})

	t.Run("create node with invalid flags", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var errors []error

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral|8,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create")
				errors = append(errors, err)
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"create",
		}, steps)

		assert.Equal(t, []error{
			ErrBadArguments,
		}, errors)
	})

	t.Run("create node with invalid acl", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var errors []error

		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			nil,
			func(resp CreateResponse, err error) {
				steps = append(steps, "create")
				errors = append(errors, err)
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"create",
		}, steps)

		assert.Equal(t, []error{
			ErrInvalidACL,
		}, errors)
	})

	t.Run("create children of non-existed parent node", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var errors []error

		c.Create(
			"/workers01/child", []byte("data02"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-child")
				errors = append(errors, err)
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"create-child",
		}, steps)

		assert.Equal(t, []error{
			ErrNoNode,
		}, errors)
	})

	t.Run("create duplicated", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var errors []error

		pathVal := "/workers01"

		c.Create(
			pathVal, []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create01")
				errors = append(errors, err)
			},
		)

		c.Create(
			pathVal, []byte("data02"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create02")
				errors = append(errors, err)
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"create01",
			"create02",
		}, steps)

		assert.Equal(t, []error{
			nil,
			ErrNodeExists,
		}, errors)
	})

	t.Run("get data with watch node not exists, do not add watch", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var errors []error

		c.Get(
			"/workers01", func(resp GetResponse, err error) {
				steps = append(steps, "get-resp")
				errors = append(errors, err)
			}, WithGetWatch(func(ev Event) {
				steps = append(steps, "get-event")
			}),
		)
		c.Create("/workers01", nil, FlagEphemeral, WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-resp")
				errors = append(errors, err)
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"get-resp",
			"create-resp",
		}, steps)
		assert.Equal(t, []error{
			ErrNoNode,
			nil,
		}, errors)
		assert.Equal(t, 0, len(c.watchers))
	})
}

func checkStat(t *testing.T, st *Stat) {
	assert.Greater(t, st.Czxid, int64(0))
	assert.Greater(t, st.Mzxid, int64(0))
	assert.Greater(t, st.Ctime, int64(0))
	assert.Greater(t, st.Mtime, int64(0))
	assert.Greater(t, st.Pzxid, int64(0))
	*st = Stat{}
}

func TestClientIntegration_Close_When_Not_Connected(t *testing.T) {
	c, err := NewClient(
		[]string{"localhost:1800"}, 30*time.Second,
		WithSessionEstablishedCallback(func(c *Client) {
		}),
	)
	if err != nil {
		panic(err)
	}

	c.Close()
	assert.Equal(t, StateDisconnected, c.state)
}

func clearZKData(c *Client) {
	var wg sync.WaitGroup
	wg.Add(1)

	c.Children("/", func(resp ChildrenResponse, err error) {
		defer wg.Done()
		if err != nil {
			panic(err)
		}
		for _, node := range resp.Children {
			if node == "zookeeper" {
				continue
			}
			p := "/" + node
			wg.Add(1)
			c.Get(p, func(resp GetResponse, err error) {
				if err != nil {
					panic(err)
				}
				c.Delete(p, resp.Stat.Version, func(resp DeleteResponse, err error) {
					if err != nil {
						fmt.Println("[ERROR] clearZKData:", err, p)
					}
					wg.Done()
				})
			})
		}
	})

	wg.Wait()
}

func TestClientIntegration_Persistence(t *testing.T) {
	t.Run("seq files", func(t *testing.T) {
		c := mustNewClient(t)

		c.Create("/workers",
			nil, 0, WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				if err != nil {
					panic(err)
				}
			},
		)

		c.Create("/workers/job-",
			[]byte("data01"), FlagEphemeral|FlagSequence, WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				if err != nil {
					panic(err)
				}
			},
		)

		c.Create("/workers/job-",
			[]byte("data02"), FlagEphemeral|FlagSequence, WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				if err != nil {
					panic(err)
				}
			},
		)

		var childrenResp ChildrenResponse
		c.Children("/workers", func(resp ChildrenResponse, err error) {
			if err != nil {
				panic(err)
			}
			childrenResp = resp
		})

		c.Close()

		slices.Sort(childrenResp.Children)
		assert.Equal(t, []string{
			"job-0000000000",
			"job-0000000001",
		}, childrenResp.Children)
	})
}

func TestClientIntegration_WithDisconnect(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		var reconnectCalls int
		c := mustNewClient(t,
			WithDialRetryDuration(100*time.Millisecond),
			WithReconnectingCallback(func(c *Client) {
				reconnectCalls++
			}),
		)

		ch := make(chan struct{})
		c.Create("/workers00",
			nil, 0, WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				close(ch)
			},
		)
		<-ch

		var steps []string
		var errors []error

		c.Exists("/workers01",
			func(resp ExistsResponse, err error) {
				errors = append(errors, err)
				steps = append(steps, "exists-resp")
			},
			WithExistsWatch(func(ev Event) {
				steps = append(steps, "exists-watch")
			}),
		)
		c.Children("/",
			func(resp ChildrenResponse, err error) {
				errors = append(errors, err)
				steps = append(steps, "children-resp")
			},
			WithChildrenWatch(func(ev Event) {
				steps = append(steps, "children-watch")
			}),
		)
		c.Get("/workers00",
			func(resp GetResponse, err error) {
				errors = append(errors, err)
				steps = append(steps, "get00-resp")
			}, WithGetWatch(func(ev Event) {
				steps = append(steps, "get-watch")
			}),
		)

		time.Sleep(500 * time.Millisecond)

		_ = c.conn.Close()

		var mut sync.Mutex
		var createErr error
		c.Create("/workers01",
			nil, 0, WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create01")
				errors = append(errors, err)
				mut.Lock()
				createErr = err
				mut.Unlock()
			},
		)

		time.Sleep(500 * time.Millisecond)

		mut.Lock()
		err := createErr
		mut.Unlock()
		assert.Equal(t, ErrConnectionClosed, err)

		c.Create("/workers01",
			nil, 0, WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create02")
				errors = append(errors, err)
				mut.Lock()
				createErr = err
				mut.Unlock()
			},
		)
		c.Delete("/workers00", 0, func(resp DeleteResponse, err error) {
			steps = append(steps, "delete-00")
			errors = append(errors, err)
		})

		c.Close()

		assert.Equal(t, nil, createErr)
		assert.Equal(t, []string{
			"exists-resp",
			"children-resp",
			"get00-resp",
			"create01",
			"exists-watch",
			"children-watch",
			"create02",
			"get-watch",
			"delete-00",
		}, steps)
		assert.Equal(t, []error{
			ErrNoNode,           // exists
			nil,                 // children
			nil,                 // get00
			ErrConnectionClosed, // create01
			nil,                 // create02
			nil,                 // delete-00
		}, errors)

		assert.Equal(t, 1, reconnectCalls)
	})

	t.Run("check creds re-apply", func(t *testing.T) {
		var reconnectCalls int
		c := mustNewClient(t,
			WithDialRetryDuration(100*time.Millisecond),
			WithReconnectingCallback(func(c *Client) {
				reconnectCalls++
			}),
		)

		var steps []string
		var errors []error

		c.AddAuth("digest", []byte("user01:password01"), func(resp AddAuthResponse, err error) {
			steps = append(steps, "add-auth")
			errors = append(errors, err)
		})

		ch := make(chan struct{})
		c.Create("/workers01",
			[]byte("data01"), FlagEphemeral, DigestACL(PermRead, "user01", "password01"),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create")
				errors = append(errors, err)
				close(ch)
			},
		)
		<-ch

		_ = c.conn.Close()

		time.Sleep(500 * time.Millisecond)

		var getResp GetResponse
		c.Get("/workers01",
			func(resp GetResponse, err error) {
				steps = append(steps, "get-resp")
				getResp = resp
				errors = append(errors, err)
			},
		)

		c.Close()

		assert.Equal(t, []string{
			"add-auth",
			"create",
			"get-resp",
		}, steps)
		assert.Equal(t, []error{
			nil,
			nil,
			nil,
		}, errors)

		assert.Equal(t, 1, reconnectCalls)
		assert.Equal(t, []byte("data01"), getResp.Data)
	})

	t.Run("exists found with watch, then disconnect then data change by set", func(t *testing.T) {
		var reconnectCalls int
		c := mustNewClient(t,
			WithDialRetryDuration(100*time.Millisecond),
			WithReconnectingCallback(func(c *Client) {
				reconnectCalls++
			}),
		)

		pathVal := "/workers01"

		ch := make(chan struct{})
		c.Create(pathVal,
			[]byte("data01"), 0, WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				close(ch)
			},
		)
		<-ch

		var steps []string
		var errors []error

		c.Exists(pathVal,
			func(resp ExistsResponse, err error) {
				steps = append(steps, "exists-resp")
				errors = append(errors, err)
			},
			WithExistsWatch(func(ev Event) {
				steps = append(steps, "exists-watch")
			}),
		)

		time.Sleep(300 * time.Millisecond)
		_ = c.conn.Close()
		time.Sleep(500 * time.Millisecond)

		c.Set(pathVal, []byte("data02"), 0, func(resp SetResponse, err error) {
			steps = append(steps, "set-new-resp")
			errors = append(errors, err)
		})

		c.Close()

		assert.Equal(t, []string{
			"exists-resp",
			"exists-watch",
			"set-new-resp",
		}, steps)
		assert.Equal(t, []error{
			nil,
			nil,
		}, errors)

		assert.Equal(t, 1, reconnectCalls)
	})

	t.Run("exists found with watch, then disconnect then data deleted", func(t *testing.T) {
		var reconnectCalls int
		c := mustNewClient(t,
			WithDialRetryDuration(100*time.Millisecond),
			WithReconnectingCallback(func(c *Client) {
				reconnectCalls++
			}),
		)

		pathVal := "/workers01"

		ch := make(chan struct{})
		c.Create(pathVal,
			[]byte("data01"), 0, WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				close(ch)
			},
		)
		<-ch

		var steps []string
		var errors []error

		c.Exists(pathVal,
			func(resp ExistsResponse, err error) {
				steps = append(steps, "exists-resp")
				errors = append(errors, err)
			},
			WithExistsWatch(func(ev Event) {
				steps = append(steps, "exists-watch")
			}),
		)

		time.Sleep(300 * time.Millisecond)
		_ = c.conn.Close()
		time.Sleep(500 * time.Millisecond)

		c.Delete(pathVal, 0, func(resp DeleteResponse, err error) {
			steps = append(steps, "delete-resp")
			errors = append(errors, err)
		})

		c.Close()

		assert.Equal(t, []string{
			"exists-resp",
			"exists-watch",
			"delete-resp",
		}, steps)
		assert.Equal(t, []error{
			nil,
			nil,
		}, errors)

		assert.Equal(t, 1, reconnectCalls)
	})

	t.Run("callback with sleep, reconnected, calls in the callback should returns error", func(t *testing.T) {
		var reconnectCalls int
		c := mustNewClient(t,
			WithDialRetryDuration(100*time.Millisecond),
			WithReconnectingCallback(func(c *Client) {
				reconnectCalls++
			}),
		)

		pathVal := "/workers01"
		var steps []string
		var errors []error

		c.Create(pathVal,
			[]byte("data01"), 0, WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create-resp")
				errors = append(errors, err)

				time.Sleep(600 * time.Millisecond)

				c.Get(pathVal, func(resp GetResponse, err error) {
					log.Println("Call Get Response")
					steps = append(steps, "get-resp")
					errors = append(errors, err)
				})
				log.Println("Call Get after Sleep")
			},
		)

		time.Sleep(300 * time.Millisecond)
		_ = c.conn.Close()
		time.Sleep(600 * time.Millisecond)

		c.Close()

		assert.Equal(t, []string{
			"create-resp",
			"get-resp",
		}, steps)
		assert.Equal(t, []error{
			nil,
			ErrConnectionClosed,
		}, errors)

		assert.Equal(t, 1, reconnectCalls)
	})
}

func TestClientInternal_WithSessionExpired(t *testing.T) {
	t.Run("session not expired because of reconnect", func(t *testing.T) {
		t.Skip()
		ch := make(chan struct{}, 1)

		var calls int

		c, err := NewClient([]string{"localhost"}, 4*time.Second,
			WithSessionEstablishedCallback(func(c *Client) {
				calls++
				select {
				case ch <- struct{}{}:
				default:
				}
			}),
		)
		if err != nil {
			panic(err)
		}
		<-ch

		time.Sleep(8 * time.Second)

		c.Close()

		assert.Equal(t, 1, calls)
	})
}

func TestClientInternal_ACL(t *testing.T) {
	t.Run("get auth failed", func(t *testing.T) {
		c := mustNewClient(t)

		var respErrors []error
		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			DigestACL(PermAll, "user01", "password0"),
			func(resp CreateResponse, err error) {
				respErrors = append(respErrors, err)
			},
		)

		var getResp GetResponse
		c.Get("/workers01", func(resp GetResponse, err error) {
			getResp = resp
			respErrors = append(respErrors, err)
		})

		c.Close()

		assert.Equal(t, []error{
			nil,
			ErrNoAuth,
		}, respErrors)
		assert.Equal(t, []byte(nil), getResp.Data)
	})

	t.Run("add auth get success", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var respErrors []error

		c.AddAuth(
			"digest", []byte("user01:password01"),
			func(resp AddAuthResponse, err error) {
				steps = append(steps, "add-auth")
				respErrors = append(respErrors, err)
			},
		)

		digestAcl := DigestACL(PermAll, "user01", "password01")
		c.Create(
			"/workers01", []byte("data01"), FlagEphemeral,
			digestAcl,
			func(resp CreateResponse, err error) {
				steps = append(steps, "create")
				respErrors = append(respErrors, err)
			},
		)

		var getResp GetResponse
		c.Get("/workers01", func(resp GetResponse, err error) {
			steps = append(steps, "get")
			getResp = resp
			respErrors = append(respErrors, err)
		})

		c.Close()

		assert.Equal(t, []string{
			"add-auth",
			"create",
			"get",
		}, steps)

		assert.Equal(t, []error{
			nil,
			nil,
			nil,
		}, respErrors)
		assert.Equal(t, []byte("data01"), getResp.Data)
	})

	t.Run("change & get auth acl without permission", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var respErrors []error

		pathVal := "/workers01"

		c.Create(
			pathVal, []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create")
				respErrors = append(respErrors, err)
			},
		)

		var aclResp []GetACLResponse

		c.GetACL(pathVal, func(resp GetACLResponse, err error) {
			steps = append(steps, "get-acl")
			respErrors = append(respErrors, err)
			aclResp = append(aclResp, resp)
		})

		c.SetACL(
			pathVal,
			DigestACL(PermRead, "user01", "password01"), 0,
			func(resp SetACLResponse, err error) {
				steps = append(steps, "set-acl")
				respErrors = append(respErrors, err)
			},
		)

		c.GetACL(pathVal, func(resp GetACLResponse, err error) {
			steps = append(steps, "get-acl")
			respErrors = append(respErrors, err)
			aclResp = append(aclResp, resp)
		})

		c.Close()

		assert.Equal(t, []string{
			"create",
			"get-acl",
			"set-acl",
			"get-acl",
		}, steps)

		assert.Equal(t, []error{
			nil,
			nil,
			nil,
			ErrNoAuth,
		}, respErrors)

		resp := aclResp[0]
		assert.Equal(t, WorldACL(PermAll), resp.ACL)
		assert.Equal(t, int32(0), resp.Stat.Aversion)
	})

	t.Run("get auth acl with permission", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var respErrors []error

		c.AddAuth(
			"digest", []byte("user01:password01"),
			func(resp AddAuthResponse, err error) {
				steps = append(steps, "add-auth")
				respErrors = append(respErrors, err)
			},
		)

		pathVal := "/workers01"

		c.Create(
			pathVal, []byte("data01"), FlagEphemeral,
			DigestACL(PermRead, "user01", "password01"),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create")
				respErrors = append(respErrors, err)
			},
		)

		var aclResp []GetACLResponse

		c.GetACL(pathVal, func(resp GetACLResponse, err error) {
			steps = append(steps, "get-acl")
			respErrors = append(respErrors, err)
			aclResp = append(aclResp, resp)
		})

		c.Close()

		assert.Equal(t, []string{
			"add-auth",
			"create",
			"get-acl",
		}, steps)

		assert.Equal(t, []error{
			nil,
			nil,
			nil,
		}, respErrors)

		resp := aclResp[0]
		assert.Equal(t, []ACL{
			{
				Perms:  PermRead,
				Scheme: "digest",
				ID:     "user01:x",
			},
		}, resp.ACL)
		assert.Equal(t, int32(0), resp.Stat.Aversion)
	})

	t.Run("get auth acl with wrong password", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var respErrors []error

		c.AddAuth(
			"digest", []byte("user01:password02"),
			func(resp AddAuthResponse, err error) {
				steps = append(steps, "add-auth")
				respErrors = append(respErrors, err)
			},
		)

		pathVal := "/workers01"

		c.Create(
			pathVal, []byte("data01"), FlagEphemeral,
			DigestACL(PermRead, "user01", "password01"),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create")
				respErrors = append(respErrors, err)
			},
		)

		var aclResp []GetACLResponse

		c.GetACL(pathVal, func(resp GetACLResponse, err error) {
			steps = append(steps, "get-acl")
			respErrors = append(respErrors, err)
			aclResp = append(aclResp, resp)
		})

		c.Close()

		assert.Equal(t, []string{
			"add-auth",
			"create",
			"get-acl",
		}, steps)

		assert.Equal(t, []error{
			nil,
			nil,
			ErrNoAuth,
		}, respErrors)

		resp := aclResp[0]
		assert.Equal(t, []ACL(nil), resp.ACL)
		assert.Equal(t, int32(0), resp.Stat.Aversion)
	})

	t.Run("set acl mismatch version", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var respErrors []error

		pathVal := "/acl_mismatch"

		c.Create(
			pathVal, []byte("data01"), FlagEphemeral,
			WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create")
				respErrors = append(respErrors, err)
			},
		)

		c.SetACL(
			pathVal,
			DigestACL(PermRead|PermWrite, "user01", "password01"), 0,
			func(resp SetACLResponse, err error) {
				steps = append(steps, "set-acl")
				respErrors = append(respErrors, err)
			},
		)

		c.SetACL(
			pathVal,
			DigestACL(PermRead, "user02", "password02"), 0,
			func(resp SetACLResponse, err error) {
				steps = append(steps, "set-acl2")
				respErrors = append(respErrors, err)
			},
		)

		c.AddAuth("digest", []byte("user01:password01"), func(resp AddAuthResponse, err error) {})

		c.Set(pathVal, []byte("new-data"), 0, func(resp SetResponse, err error) {
			steps = append(steps, "set-data")
			respErrors = append(respErrors, err)
		})

		c.Close()

		assert.Equal(t, []string{
			"create",
			"set-acl",
			"set-acl2",
			"set-data",
		}, steps)

		assert.Equal(t, []error{
			nil,
			nil,
			ErrBadVersion,
			nil,
		}, respErrors)
	})
}

func TestClientIntegration_Ping(t *testing.T) {
	c := mustNewClient(t)

	c.sendPingRequest()
	time.Sleep(300 * time.Millisecond)

	c.Close()

	assert.Equal(t, 0, len(c.recvMap))
}

func TestClientIntegration_Ping_Multi_Times(t *testing.T) {
	c := mustNewClient(t)

	c.pingSignalChan <- struct{}{}
	time.Sleep(300 * time.Millisecond)

	c.Close()

	assert.Equal(t, 0, len(c.recvMap))
}

func TestClientIntegration_Delete_Empty_Node(t *testing.T) {
	c := mustNewClient(t)

	var errors []error
	c.Delete("/workers01", 0, func(resp DeleteResponse, err error) {
		errors = append(errors, err)
	})

	c.Close()

	assert.Equal(t, []error{
		ErrNoNode,
	}, errors)
}

func TestClientIntegration_Delete_Node_With_Children(t *testing.T) {
	c := mustNewClient(t)

	var errors []error
	c.Create("/workers01", nil, 0, WorldACL(PermAll), func(resp CreateResponse, err error) {
		errors = append(errors, err)
	})

	c.Create("/workers01/data", nil, 0, WorldACL(PermAll), func(resp CreateResponse, err error) {
		errors = append(errors, err)
	})

	c.Delete("/workers01", 0, func(resp DeleteResponse, err error) {
		errors = append(errors, err)
	})
	c.Delete("/workers01/data", 0, func(resp DeleteResponse, err error) {
		errors = append(errors, err)
	})

	c.Close()

	assert.Equal(t, []error{
		nil,
		nil,
		ErrNotEmpty,
		nil,
	}, errors)
}

func TestClientIntegration_WithInvalidPath(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		c := mustNewClient(t)

		var errors []error
		c.Get("/hello/", func(resp GetResponse, err error) {
			errors = append(errors, err)
		})

		c.Close()

		assert.Equal(t, []error{
			ErrInvalidPath,
		}, errors)
	})

	t.Run("create", func(t *testing.T) {
		c := mustNewClient(t)

		var errors []error
		c.Create("/hello/", nil, 0, WorldACL(PermAll), func(resp CreateResponse, err error) {
			errors = append(errors, err)
		})

		c.Close()

		assert.Equal(t, []error{
			ErrInvalidPath,
		}, errors)
	})

	t.Run("create-ephemeral", func(t *testing.T) {
		c := mustNewClient(t)

		var errors []error
		c.Create("/hello/", nil, FlagEphemeral|FlagSequence, WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				errors = append(errors, err)
			},
		)

		c.Close()

		assert.Equal(t, []error{
			ErrNoNode,
		}, errors)
	})

	t.Run("children", func(t *testing.T) {
		c := mustNewClient(t)

		var errors []error
		c.Children("/hello/", func(resp ChildrenResponse, err error) {
			errors = append(errors, err)
		})

		c.Close()

		assert.Equal(t, []error{
			ErrInvalidPath,
		}, errors)
	})

	t.Run("exists", func(t *testing.T) {
		c := mustNewClient(t)

		var errors []error
		c.Exists("/hello/", func(resp ExistsResponse, err error) {
			errors = append(errors, err)
		})

		c.Close()

		assert.Equal(t, []error{
			ErrInvalidPath,
		}, errors)
	})

	t.Run("delete", func(t *testing.T) {
		c := mustNewClient(t)

		var errors []error
		c.Delete("/hello/", 0, func(resp DeleteResponse, err error) {
			errors = append(errors, err)
		})

		c.Close()

		assert.Equal(t, []error{
			ErrInvalidPath,
		}, errors)
	})

	t.Run("set acl", func(t *testing.T) {
		c := mustNewClient(t)

		var errors []error
		c.SetACL("/hello/", WorldACL(PermAll), 0, func(resp SetACLResponse, err error) {
			errors = append(errors, err)
		})

		c.Close()

		assert.Equal(t, []error{
			ErrInvalidPath,
		}, errors)
	})

	t.Run("get acl", func(t *testing.T) {
		c := mustNewClient(t)

		var errors []error
		c.GetACL("/hello/", func(resp GetACLResponse, err error) {
			errors = append(errors, err)
		})

		c.Close()

		assert.Equal(t, []error{
			ErrInvalidPath,
		}, errors)
	})
}

type testLogger struct {
}

func (*testLogger) Infof(format string, args ...any) {
	log.Printf("[TEST] [INFO] [ZK] "+format, args...)
}

func (*testLogger) Warnf(format string, args ...any) {
	log.Printf("[TEST] [WARN] [ZK] "+format, args...)
}

func (*testLogger) Errorf(format string, args ...any) {
	log.Printf("[TEST] [ERROR] [ZK] "+format, args...)
}

func TestClientIntegration_Connect_Same_Session_On_Two_Clients(t *testing.T) {
	retryConnectCount := 0

	sess1Chan := make(chan struct{})
	c1, err := NewClient([]string{"localhost"}, 30*time.Second,
		WithSessionEstablishedCallback(func(c *Client) {
			close(sess1Chan)
		}),
		WithReconnectingCallback(func(c *Client) {
			retryConnectCount++
		}),
	)
	if err != nil {
		panic(err)
	}

	<-sess1Chan

	c1.mut.Lock()
	sessionID := c1.sessionID
	passwd := c1.passwd
	lastZxid := c1.lastZxid
	c1.mut.Unlock()

	c2, err := newClientInternal([]string{"localhost"}, 30*time.Second, WithLogger(&testLogger{}))
	if err != nil {
		panic(err)
	}

	c2.sessionID = sessionID
	c2.passwd = passwd
	c2.lastZxid = lastZxid

	fmt.Println("=========================")

	connOutput := c2.doConnect()
	assert.Equal(t, false, connOutput.closed)
	assert.Equal(t, false, connOutput.needRetry)
	conn := connOutput.conn
	assert.NotNil(t, conn)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c2.runReceiver(conn)
	}()

	time.Sleep(300 * time.Millisecond)

	_ = conn.Close()

	wg.Wait()
	if c2.conn != nil {
		_ = c2.conn.Close()
	}

	c1.Close()

	assert.Equal(t, 1, retryConnectCount)
}

func TestClientIntegration_Use_After_Close(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		c := mustNewClient(t)

		c.Close()

		var calls int
		c.Get("/workers", func(resp GetResponse, err error) {
			calls++
		})
		assert.Equal(t, 0, calls)
		assert.Equal(t, 0, len(c.sendQueue))
	})
}

func TestClientIntegration_Create_And_Watch_For_Its_Own_Deletion(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		c := mustNewClient(t)

		var steps []string
		var errors []error
		c.Create("/workers", nil, FlagEphemeral, WorldACL(PermAll),
			func(resp CreateResponse, err error) {
				steps = append(steps, "create")
				errors = append(errors, err)
			},
		)
		c.Get("/workers", func(resp GetResponse, err error) {
			steps = append(steps, "get-resp")
			errors = append(errors, err)
		}, WithGetWatch(func(ev Event) {
			steps = append(steps, "get-watch")
		}))

		c.Close()

		assert.Equal(t, []string{"create", "get-resp", "get-watch"}, steps)
		assert.Equal(t, []error{nil, nil}, errors)
	})
}

func newClientIntegrationWithoutPing() *Client {
	ch := make(chan struct{}, 1)

	opts := []Option{
		WithSessionEstablishedCallback(func(c *Client) {
			select {
			case ch <- struct{}{}:
			default:
			}
		}),
	}

	c, err := newClientInternal([]string{"localhost"}, 12*time.Second, opts...)
	if err != nil {
		panic(err)
	}

	c.wg.Add(2)

	go func() {
		defer c.wg.Done()
		c.connectAndRunTCPHandlers()
	}()

	go func() {
		defer c.wg.Done()
		c.runHandler()
	}()

	<-ch

	clearZKData(c)

	return c
}

func TestClientIntegration__Session_Expired(t *testing.T) {
	t.Skip()

	c := newClientIntegrationWithoutPing()

	time.Sleep(1 * time.Second)
	c.recvTimeout.Store(int64(30 * time.Second))
	c.sendPingRequest()
	log.Println("[TEST] Sent Ping")

	time.Sleep(16 * time.Second)

	var errors []error
	c.Create("/workers01", nil, 0, WorldACL(PermAll), func(resp CreateResponse, err error) {
		log.Println("[TEST] Handle Create Resp:", err)
		errors = append(errors, err)
	})

	c.Close()

	assert.Equal(t, []error{
		nil,
	}, errors)
}
