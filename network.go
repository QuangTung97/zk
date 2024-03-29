package zk

import (
	"net"
	"time"
)

type tcpConnImpl struct {
	conn net.Conn
}

func NewTCPConn(conn net.Conn) NetworkConn {
	return &tcpConnImpl{
		conn: conn,
	}
}

var _ NetworkConn = &tcpConnImpl{}

func (c *tcpConnImpl) Write(p []byte) (int, error) {
	return c.conn.Write(p)
}

func (c *tcpConnImpl) Read(p []byte) (int, error) {
	return c.conn.Read(p)
}

func (c *tcpConnImpl) SetReadDeadline(d time.Duration) error {
	return c.conn.SetReadDeadline(time.Now().Add(d))
}

func (c *tcpConnImpl) SetWriteDeadline(d time.Duration) error {
	return c.conn.SetWriteDeadline(time.Now().Add(d))
}

func (c *tcpConnImpl) Close() error {
	return c.conn.Close()
}
