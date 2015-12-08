package gnet

import (
	"testing"

	"github.com/yuyang0/gkv/pkg/utils/log"
)

var (
	s    *TcpServer
	c    *TcpClient
	addr string
)

func init() {
	log.SetLevel(log.LEVEL_DEBUG)
	log.Debugf("set log level to debug..")
	addr = "127.0.0.1:8888"
	s = NewTcpServer(addr)
	c = NewTcpClient()
	go s.Start()

	go func() {
		for reqMsg := range s.reqChan {
			respMsg := NewRespMsg(ENCODE_TYPE_JSON, reqMsg.sessionId, 2, reqMsg.data)
			conn := reqMsg.connection
			go conn.SendMsg(respMsg)
		}
	}()
}

func TestServer(t *testing.T) {
	log.Debugf("Enter TestServer..")
	for i := 0; i < 2000; i++ {
		log.Debugf("loop: %d.", i)
		val := "hello world"
		msg := NewReqMsg(ENCODE_TYPE_JSON, 2, []byte(val))
		log.Debugf("request msg: %s", msg.String())
		c.SendMsg(addr, msg)
		respMsg, _ := c.GetRespBlock(msg.sessionId)
		if string(respMsg.data) != val {
			t.Error("not match ", val, string(respMsg.data))
		}
	}
}
