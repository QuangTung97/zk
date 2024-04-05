package concurrency

import (
	"errors"
	"strconv"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
)

type simpleCounter struct {
	client curator.FakeClientID
}

func newSimpleCounter(client curator.FakeClientID) *simpleCounter {
	return &simpleCounter{
		client: client,
	}
}

func (l *simpleCounter) isLeader(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.Get("/counter", func(resp zk.GetResponse, err error) {
			if err != nil {
				if errors.Is(err, zk.ErrNoNode) {
					l.increase(sess, 1, 0)
					return
				}
				if errors.Is(err, zk.ErrConnectionClosed) {
					sess.AddRetry(l.isLeader)
					return
				}
				panic(err)
			}

			num, err := strconv.ParseInt(string(resp.Data), 10, 64)
			if err != nil {
				panic(err)
			}
			l.increase(sess, int(num)+1, resp.Stat.Version)
		})
	})
}

func numToBytes(val int) []byte {
	return []byte(strconv.FormatInt(int64(val), 10))
}

func (l *simpleCounter) setCounterResp(sess *curator.Session) func(_ zk.SetResponse, err error) {
	return func(_ zk.SetResponse, err error) {
		if err != nil {
			if errors.Is(err, zk.ErrConnectionClosed) {
				sess.AddRetry(l.isLeader)
				return
			}
			panic(err)
		}
		l.isLeader(sess)
	}
}

func (l *simpleCounter) createCounterResp(sess *curator.Session) func(_ zk.CreateResponse, err error) {
	return func(_ zk.CreateResponse, err error) {
		if err != nil {
			if errors.Is(err, zk.ErrConnectionClosed) {
				sess.AddRetry(l.isLeader)
				return
			}
			panic(err)
		}
		l.isLeader(sess)
	}
}

func (l *simpleCounter) increase(sess *curator.Session, nextVal int, version int32) {
	sess.Run(func(client curator.Client) {
		if nextVal > 1 {
			client.Set("/counter", numToBytes(nextVal), version, l.setCounterResp(sess))
		} else {
			client.Create("/counter", numToBytes(nextVal), 0, l.createCounterResp(sess))
		}
	})
}
