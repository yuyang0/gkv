package common

import (
	"bufio"
	"io"
	"net"
)

type Connection struct {
	incoming chan *Msg
	outgoing chan *Msg
	reader   *bufio.Reader
	writer   *bufio.Writer
	conn     net.Conn
	timeout  int
}

func NewConnection(conn net.Conn) *Connection {
	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	connection := &Connection{
		incoming: make(chan *Msg),
		outgoing: make(chan *Msg),
		reader:   reader,
		writer:   writer,
		conn:     conn,
	}

	go connection.read()
	go connection.write()

	return connection
}

func (connection *Connection) read() {
	for {
		msg, err := ReadMsg(connection.reader)
		if err != nil {
			if err == io.EOF {
				close(connection.incoming)
				return
			}
		}
		connection.incoming <- msg
	}
}

func (conn *Connection) write() {
	for msg := range conn.outgoing {
		data := msg.ConvertToBytes()
		n, err := conn.writer.Write(data)
		if err != nil {
			return
		}
		conn.writer.Flush()
	}
}

func (conn *Connection) WriteMsgToChan(msg *Msg) {
	conn.outgoing <- msg
}

func (conn *Connection) ReadMsgFromChan() *Msg {
	msg, ok := <-conn.incoming
	if ok {
		return msg
	} else {
		return nil
	}
}

func (conn *Connection) Close() {
	close(conn.incoming)
	close(conn.outgoing)
	conn.conn.Close()
}
