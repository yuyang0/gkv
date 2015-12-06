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

func (client *Connection) Listen() {
	go client.Read()
	go client.Write()
}

func NewConnection(connection net.Conn) *Connection {
	writer := bufio.NewWriter(connection)
	reader := bufio.NewReader(connection)

	info := &Connection{
		incoming: make(chan *Msg),
		outgoing: make(chan *Msg),
		reader:   reader,
		writer:   writer,
	}

	info.Listen()

	return info
}

func (client *Connection) Read() {
	for {
		msg := ReadMsg(client.reader)
		client.incoming <- msg
	}
}

func (client *Connection) Write() {
	for msg := range client.outgoing {
		data := msg.ConvertToBytes()
		n, err := client.writer.Write(data)
		if err != nil {
			return
		}
		client.writer.Flush()
	}
}

func (info *Connection) WriteMsgToChan(msg *Msg) {
	info.outgoing <- msg
}

func (info *Connection) ReadMsgFromChan() *Msg {
	msg := <-info.incoming
	return msg
}
