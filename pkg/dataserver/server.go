package dataserver

import (
	"github.com/yuyang0/gkv/pkg/gnet"
)

type DataServer struct {
	server *gnet.TcpServer
	client *gnet.TcpClient
}

func NewDataServer() *DataServer {
	return nil
}
