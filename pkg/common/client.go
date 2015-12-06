package common

import (
	"github.com/yuyang0/gkv/pkg/utils/log"
	"net"
	"sync"
)

type TcpClient struct {
	mu       sync.Mutex
	conn_map map[string]*Connection
}

func NewTcpClient() *TcpClient {
	return &TcpClient{
		conn_map: make(map[string]*Connection, 10),
	}
}

func (client *TcpClient) SendMsg(host string, port int, msg *Msg) bool {
	addr := host + ":" + string(port)
	conn, ok := client.conn_map[addr]
	if ok {
		conn.WriteMsgToChan(msg)
	} else {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.WarnErrorf(err, "Can't dail to %s", addr)
			return false
		}
		connection := NewConnection(conn)
		client.conn_map[addr] = connection
		connection.WriteMsgToChan(msg)
	}
	return true
}
