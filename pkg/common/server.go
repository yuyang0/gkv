package common

import (
	"bufio"
	"fmt"
	"net"
)

type ConnInfo struct {
	incoming chan *Msg
	outgoing chan *Msg
	reader   *bufio.Reader
	writer   *bufio.Writer
	conn     net.Conn
}

func (client *ConnInfo) Listen() {
	go client.Read()
	go client.Write()
}

func NewConnInfo(connection net.Conn) *ConnInfo {
	writer := bufio.NewWriter(connection)
	reader := bufio.NewReader(connection)

	info := &ConnInfo{
		incoming: make(chan *Msg),
		outgoing: make(chan *Msg),
		reader:   reader,
		writer:   writer,
	}

	info.Listen()

	return info
}

func (client *ConnInfo) Read() {
	for {
		msg := ReadMsg(client.reader)
		client.incoming <- msg
	}
}

func (client *ConnInfo) Write() {
	for msg := range client.outgoing {
		data := msg.ConvertToBytes()
		n, err := client.writer.Write(data)
		if err != nil {
			return
		}
		client.writer.Flush()
	}
}

func (info *ConnInfo) WriteMsgToChan(msg *Msg) {
	info.outgoing <- msg
}

type TcpServer struct {
	host      string
	port      int
	listener  net.Listener
	conn_map  map[string]*ConnInfo
	conn_chan chan *Msg
}

func NewTcpServer(host string, port int) *TcpServer {
	addr := host + ":" + string(port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil
	}
	server := &TcpServer{host, port, listener, make(map[string]*ConnInfo), make(chan *Msg)}
	return server
}

func (server *TcpServer) Loop() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			fmt.Printf("error: accept")
			continue
		}
		go func(conn net.Conn) {
			addr := conn.RemoteAddr().String()
			info := NewConnInfo(conn)
			server.conn_map[addr] = info

			for {
				msg := <-info.incoming
				go HandleMsg(info, msg)
			}
		}(conn)
	}
}

func HandleMsg(conn *ConnInfo, msg *Msg) {
}
