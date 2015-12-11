package gnet

import (
	"bufio"
	"net"
	"sync"
	"time"

	"github.com/yuyang0/gkv/pkg/utils/log"
)

type Connection struct {
	incoming    chan *Msg
	outgoing    chan *Msg
	reader      *bufio.Reader
	writer      *bufio.Writer
	conn        net.Conn
	addr        string
	lastUseTime time.Time

	mtx sync.Mutex
}

func NewConnection(conn net.Conn) *Connection {
	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	connection := &Connection{
		incoming:    make(chan *Msg),
		outgoing:    make(chan *Msg),
		reader:      reader,
		writer:      writer,
		conn:        conn,
		addr:        conn.RemoteAddr().String(),
		lastUseTime: time.Now(),
	}

	go connection.read()
	go connection.write()

	return connection
}

func (c *Connection) read() {
	for {
		c.conn.SetReadDeadline(time.Now().Add(70 * time.Second))
		msg, err := readMsgFromReader(c.reader)
		if err != nil {
			log.ErrorErrorf(err, "Can't read Msg from %s", c.addr)

			close(c.incoming)
			c.conn.Close()
			return
		}

		c.mtx.Lock()
		c.lastUseTime = time.Now()
		c.mtx.Unlock()

		c.incoming <- msg
	}
}

func (c *Connection) write() {
	for msg := range c.outgoing {
		log.Debugf("write message(%d)", msg.sessionId)
		data := msg.ConvertToBytes()
		c.conn.SetWriteDeadline(time.Now().Add(70 * time.Second))
		_, err := c.writer.Write(data)
		if err != nil {
			log.ErrorErrorf(err, "Can't write all data to connection")
			return
		}
		c.writer.Flush()

		log.Debugf("finished write message(%d)", msg.sessionId)

		c.mtx.Lock()
		c.lastUseTime = time.Now()
		c.mtx.Unlock()
	}
}

// send a message(this function maybe block)
func (conn *Connection) SendMsg(msg *Msg) {
	conn.outgoing <- msg
}

// receive a msg from conncetion(this function maybe block.)
func (conn *Connection) ReceiveMsg() *Msg {
	msg, ok := <-conn.incoming
	if ok {
		return msg
	} else {
		return nil
	}
}

func (c *Connection) Disconnect() {
	close(c.outgoing)
	c.conn.Close()
}

func (conn *Connection) CloseSendChan() {
	close(conn.outgoing)
}

func (conn *Connection) CloseReceiveChan() {
	close(conn.incoming)
}

func (conn *Connection) Cleanup() {
	close(conn.incoming)
	close(conn.outgoing)
	conn.conn.Close()
}
