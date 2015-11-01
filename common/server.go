package common

import (
	"fmt"
	"net"
)

type TcpServer struct {
	host     string
	port     int
	listener net.Listener
}

func NewTcpServer(host string, port int) *TcpServer {
	addr := host + ":" + string(port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil
	}
	server := &TcpServer{host, port, listener}
	return server
}

/*
 * handle the incomeing connections, we need parse the buffer according
 * to the protocol.
 */
func (server *TcpServer) handle(conn net.Conn) {

}

func (server *TcpServer) Loop() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			fmt.Printf("error: accept")
			continue
		}
		go server.handle(conn)
	}
}
