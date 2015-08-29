package common

import (
	"fmt"
	"net"
)

type connHandler func(conn net.Conn)
type TcpServer struct {
	host     string
	port     int
	listener net.Listener
	handler  connHandler
}

func NewTcpServer(host string, port int, handler connHandler) *TcpServer {
	addr := host + ":" + string(port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil
	}
	server := &TcpServer{host, port, listener, handler}
	return server
}

func (server *TcpServer) loop() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			fmt.Printf("error: accept")
			continue
		}
		go server.handler(conn)
	}
}
