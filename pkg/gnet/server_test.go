package gnet

import (
	"testing"
)

var (
	s    *TcpServer
	c    *TcpClient
	addr string
)

func init() {
	addr = "127.0.0.1:8888"
	s = NewTcpServer(addr)
	c = NewTcpClient()
	go s.Start()

	go func() {
		for reqMsg := range s.reqChan {
			respMsg := NewRespMsg(ENCODE_TYPE_JSON, reqMsg.sessionId, 2, reqMsg.data)
			conn := reqMsg.connection
			conn.SendMsg(respMsg)
		}
	}()
}

func TestServer(t *testing.T) {
	for i := 0; i < 5000; i++ {
		val := "hello world"
		msg := NewReqMsg(ENCODE_TYPE_JSON, 2, []byte(val))
		c.SendMsg(addr, msg)
		respMsg, _ := c.GetRespBlock(msg.sessionId)
		if string(respMsg.data) != val {
			t.Error("not match ", val, string(respMsg.data))
		}
	}
}
