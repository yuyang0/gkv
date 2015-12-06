package common

import (
	"bufio"
	"net"
)

type Connection struct {
	incoming chan *Msg
	outgoing chan *Msg
	reader   *bufio.Reader
	writer   *bufio.Writer
	conn     net.Conn
}

func (conn *Connection) Listen() {
	go conn.Read()
	go conn.Write()
}

func NewConnection(conn net.Conn) *Connection {
	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	info := &Connection{
		incoming: make(chan *Msg),
		outgoing: make(chan *Msg),
		reader:   reader,
		writer:   writer,
	}

	info.Listen()

	return info
}

func (conn *Connection) Read() {
	for {
		msg := ReadMsg(conn.reader)
		conn.incoming <- msg
	}
}

func (conn *Connection) Write() {
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
	msg := <-conn.incoming
	return msg
}

func (conn *Connection) Close() {
	close(conn.incoming)
	close(conn.outgoing)
	conn.conn.Close()
}
