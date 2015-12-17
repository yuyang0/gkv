package configserver

import (
	"github.com/yuyang0/gkv/pkg/gnet"
)

type ConfigServer struct {
	server *gnet.TcpServer
	client *gnet.TcpClient
}

func NewConfigServer() *ConfigServer {
	return nil
}
