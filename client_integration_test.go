//go:build integration

package zk

import (
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type tcpConnTest struct {
	conn net.Conn
}

func (c *tcpConnTest) Read(p []byte) (n int, err error) {
	return c.conn.Read(p)
}

func (c *tcpConnTest) Write(p []byte) (n int, err error) {
	return c.conn.Write(p)
}

func (c *tcpConnTest) SetReadDeadline(d time.Duration) error {
	return c.conn.SetReadDeadline(time.Now().Add(d))
}

func (c *tcpConnTest) SetWriteDeadline(d time.Duration) error {
	return c.conn.SetWriteDeadline(time.Now().Add(d))
}

func (c *tcpConnTest) Close() error {
	return c.conn.Close()
}

func TestClientIntegration_Authenticate(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		c, err := newClientInternal([]string{"localhost"}, 12*time.Second)
		assert.Equal(t, nil, err)

		netConn, err := net.Dial("tcp", "localhost:2181")
		assert.Equal(t, nil, err)
		defer func() { _ = netConn.Close() }()

		err = c.authenticate(&tcpConnTest{
			conn: netConn,
		})
		assert.Equal(t, nil, err)

		fmt.Println("SESSION_ID:", c.sessionID)
		fmt.Println("PASS:", c.passwd)
		assert.Equal(t, int32(12_000), c.sessionTimeoutMs)
	})
}

func (c *tcpConnTest) closeSession(client *Client) {
	client.enqueueRequest(
		opClose,
		&closeRequest{},
		&closeResponse{},
		nil,
	)

	reqs, _ := client.getFromSendQueue()
	err := client.sendData(c, reqs[0])
	if err != nil {
		panic(err)
	}

	client.readSingleData(c)
}

func TestClientIntegration_Authenticate_And_Create(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		c, err := newClientInternal([]string{"localhost"}, 12*time.Second)
		assert.Equal(t, nil, err)

		netConn, err := net.Dial("tcp", "localhost:2181")
		assert.Equal(t, nil, err)
		defer func() { _ = netConn.Close() }()

		connTest := &tcpConnTest{
			conn: netConn,
		}
		defer connTest.closeSession(c)

		// do authenticate
		err = c.authenticate(connTest)
		assert.Equal(t, nil, err)

		req := &CreateRequest{
			Path:  "/workers",
			Data:  []byte("data01"),
			Acl:   WorldACL(PermAll),
			Flags: FlagEphemeral,
		}

		c.enqueueRequest(
			opCreate,
			req,
			&createResponse{},
			nil,
		)

		reqs, ok := c.getFromSendQueue()
		assert.Equal(t, true, ok)
		assert.Equal(t, 1, len(reqs))

		// Do Send
		err = c.sendData(connTest, reqs[0])
		assert.Equal(t, nil, err)

		// Recv Response
		c.readSingleData(connTest)
		assert.Equal(t, 1, len(c.handleQueue))

		c.handleQueue[0].zxid = 0
		assert.Equal(t, handleEvent{
			state: StateHasSession,
			req: clientRequest{
				xid:    1,
				opcode: opCreate,
				request: &CreateRequest{
					Path:  "/workers",
					Data:  []byte("data01"),
					Acl:   WorldACL(PermAll),
					Flags: 1,
				},
				response: &createResponse{
					Path: "/workers",
				},
			},
		}, c.handleQueue[0])
	})
}

func mustNewClient(_ *testing.T) *Client {
	ch := make(chan struct{})
	c, err := NewClient(
		[]string{"localhost"}, 30*time.Second,
		WithSessionEstablishedCallback(func() {
			close(ch)
		}),
	)
	if err != nil {
		panic(err)
	}
	<-ch
	return c
}

func TestClient_Create_And_Get(t *testing.T) {
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
}

func checkStat(t *testing.T, st *Stat) {
	assert.Greater(t, st.Czxid, int64(0))
	assert.Greater(t, st.Mzxid, int64(0))
	assert.Greater(t, st.Ctime, int64(0))
	assert.Greater(t, st.Mtime, int64(0))
	assert.Greater(t, st.Pzxid, int64(0))
	*st = Stat{}
}
