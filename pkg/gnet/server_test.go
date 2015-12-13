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
	// log.SetLevel(log.LEVEL_DEBUG)
	log.SetLevel(log.LEVEL_ERROR)
	log.Debugf("set log level to debug..")
	addr = "127.0.0.1:8888"
	s = NewTcpServer(addr)
	c = NewTcpClient()
	go s.Start()

	go func() {
		for reqMsg := range s.reqChan {
			respMsg := NewRespMsg(ENCODE_TYPE_JSON, reqMsg.sessionId, 3, reqMsg.data)
			conn := reqMsg.connection
			go conn.SendMsg(respMsg)
		}
	}()
}

func TestServer(t *testing.T) {
	log.Debugf("Enter TestServer..")
	numTimeout := 0
	numUnmatch := 0
	numSuccess := 0
	for i := 0; i < 100000; i++ {
		log.Debugf("loop: %d.", i)
		val := "hello world"
		msg := NewReqMsg(ENCODE_TYPE_JSON, 3, []byte(val))
		log.Debugf("request msg: %s", msg.String())
		c.SendReq(addr, msg)
		respMsg, _ := c.GetRespBlock(msg.sessionId)
		if respMsg.pCode == MSG_TIMEOUT {
			numTimeout++
			continue
		}
		if string(respMsg.data) != val {
			numUnmatch++
			t.Error("not match ", val, string(respMsg.data))
		} else {
			numSuccess++
		}
	}
	log.Infof("\nnumUnmatch: %d\nnumTimeout: %d\nnumSuccess: %d", numUnmatch, numTimeout, numSuccess)
}
