package zk

import (
	"bufio"
	"net"
	"time"
)

type Flusher interface {
	Flush() error
}

type tcpConnImpl struct {
	writer *bufio.Writer
	conn   net.Conn
}

func NewTCPConn(conn net.Conn) NetworkConn {
	return &tcpConnImpl{
		writer: bufio.NewWriter(conn),
		conn:   conn,
	}
}

var _ NetworkConn = &tcpConnImpl{}

func (c *tcpConnImpl) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

func (c *tcpConnImpl) Flush() error {
	return c.writer.Flush()
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
