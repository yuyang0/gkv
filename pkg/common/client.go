package common

import (
	"net"

	"github.com/yuyang0/gkv/pkg/utils/log"
)

type TcpClient struct {
	host string
	port int
	conn net.Conn
}

func NewTcpClient(host string, port int) *TcpClient {
	addr := host + ":" + string(port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.WarnErrorf(err, "Can't dail to %s", addr)
		return nil
	}
	client := &TcpClient{host, port, conn}
	return client
}

type ConnMgr struct {
}
