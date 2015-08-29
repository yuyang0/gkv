package common

import (
	"fmt"
	"net"
)

type TcpClient struct {
	ServerHost string
	ServerPort int
	conn       net.Conn
}

func NewTcpClient(host string, port int) *TcpClient {
	addr := host + ":" + string(port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("error: client")
		return nil
	}
	client := &TcpClient{host, port, conn}
	return client
}
