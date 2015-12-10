package gnet

import (
	"bufio"
	"net"
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
	isConnected bool
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
		isConnected: true,
	}

	go connection.read()
	go connection.write()

	return connection
}

func (connection *Connection) read() {
	for {
		if connection.isConnected == false {
			close(connection.incoming)
			connection.conn.Close()
			return
		}
		connection.conn.SetReadDeadline(time.Now().Add(70 * time.Second))
		msg, err := readMsgFromReader(connection.reader)
		if err != nil {
			log.ErrorErrorf(err, "Can't read Msg from %s", connection.addr)
			connection.isConnected = false
			close(connection.incoming)
			connection.conn.Close()
			return
		}
		connection.lastUseTime = time.Now()
		connection.incoming <- msg
	}
}

func (conn *Connection) write() {
	for msg := range conn.outgoing {
		if conn.isConnected == false {
			return
		}
		data := msg.ConvertToBytes()
		conn.conn.SetWriteDeadline(time.Now().Add(70 * time.Second))
		_, err := conn.writer.Write(data)
		if err != nil {
			conn.isConnected = false
			log.ErrorErrorf(err, "Can't write all data to connection")
			return
		}
		conn.writer.Flush()
		conn.lastUseTime = time.Now()
	}
}

// send a message(this function maybe block)
func (conn *Connection) SendMsg(msg *Msg) {
	if conn.isConnected == false {
		return
	} else {
		conn.outgoing <- msg
	}
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

func (conn *Connection) Disconnect() {
	conn.isConnected = false
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
