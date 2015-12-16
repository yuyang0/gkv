package gnet

import (
	"bufio"
	"net"
	"sync"
	"time"

	"github.com/yuyang0/gkv/pkg/utils/log"
)

const (
	DEFAULT_SPEED_LIMIT = 1000
)

type Connection struct {
	isServer    bool
	speedLimit  int // limit the max number of packets per second
	incoming    chan *Msg
	outgoing    chan *Msg
	reader      *bufio.Reader
	writer      *bufio.Writer
	conn        net.Conn
	addr        string
	lastUseTime time.Time

	mtx            sync.Mutex
	disconnectChan chan bool
}

func NewConnection(conn net.Conn, isServer bool) *Connection {
	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	connection := &Connection{
		isServer:    isServer,
		speedLimit:  1000,
		incoming:    make(chan *Msg),
		outgoing:    make(chan *Msg),
		reader:      reader,
		writer:      writer,
		conn:        conn,
		addr:        conn.RemoteAddr().String(),
		lastUseTime: time.Now(),

		disconnectChan: make(chan bool, 1),
	}

	go connection.read()
	go connection.write()

	return connection
}

func (c *Connection) read() {
	curNumMsg := 0
	lastTime := time.Now()
	for {
		// check if need to stop this goroutine
		select {
		case <-c.disconnectChan:
			close(c.incoming)
			return
		default:
		}
		// c.conn.SetReadDeadline(time.Now().Add(70 * time.Second))
		msg, err := readMsgFromReader(c.reader)
		if err != nil {
			log.ErrorErrorf(err, "Can't read Msg from %s", c.addr)

			close(c.incoming)
			c.Disconnect()
			return
		}

		c.mtx.Lock()
		c.lastUseTime = time.Now()
		c.mtx.Unlock()

		c.incoming <- msg

		// limit the read speed
		curNumMsg++
		now := time.Now()
		d := now.Sub(lastTime)
		if d.Seconds() >= 1 {
			lastTime = now
			curNumMsg = 0
		}
		if curNumMsg > c.speedLimit {
			time.Sleep(d)
		}
	}
}

func (c *Connection) write() {
	curNumMsg := 0
	lastTime := time.Now()

	for {
		select {
		case <-c.disconnectChan:
			return
		case msg, ok := <-c.outgoing:
			if !ok {
				return
			}
			data := msg.ConvertToBytes()
			// c.conn.SetWriteDeadline(time.Now().Add(70 * time.Second))
			_, err := c.writer.Write(data)
			if err != nil {
				log.ErrorErrorf(err, "Can't write all data to connection")
				c.Disconnect()
				return
			}
			c.writer.Flush()

			// log.Debugf("finished write message(%d)", msg.sessionId)

			c.mtx.Lock()
			c.lastUseTime = time.Now()
			c.mtx.Unlock()

			// limit the write speed
			curNumMsg++
			now := time.Now()
			d := now.Sub(lastTime)
			if d.Seconds() >= 1 {
				lastTime = now
				curNumMsg = 0
			}
			if curNumMsg > c.speedLimit {
				time.Sleep(d)
			}
		}
	}
}

func (c *Connection) LastUseTime() time.Time {
	c.mtx.Lock()
	lastUseTime := c.lastUseTime
	c.mtx.Unlock()
	return lastUseTime
}

// send a message(this function maybe block)
func (c *Connection) SendMsg(msg *Msg) bool {
	if c.IsDisconnect() {
		return false
	}
	c.outgoing <- msg
	return true
}

func (c *Connection) SendMsgNonBlock(msg *Msg) bool {
	if c.IsDisconnect() {
		return false
	}
	go func() {
		c.outgoing <- msg
	}()
	return true
}

// receive a msg from conncetion(this function maybe block.)
func (c *Connection) ReceiveMsg() *Msg {
	if c.IsDisconnect() {
		return nil
	}
	msg, ok := <-c.incoming
	if ok {
		return msg
	} else {
		return nil
	}
}

func (c *Connection) IsDisconnect() bool {
	if len(c.disconnectChan) > 0 {
		return true
	} else {
		return false
	}
}

func (c *Connection) Disconnect() {
	select {
	case c.disconnectChan <- true:
	default:
		// log.Warnf("You may call Disconnect on connection(%s) twice", c.addr)
		// return
	}
	// maybe a error, but we don't care..
	c.conn.Close()
	// close(c.outgoing)
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
