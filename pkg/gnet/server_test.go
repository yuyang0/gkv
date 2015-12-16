package gnet

import (
	"math/rand"
	"testing"
	"time"

	"github.com/yuyang0/gkv/pkg/utils/log"
)

var (
	s    *TcpServer
	c    *TcpClient
	addr string
)

func init() {
	rand.Seed(time.Now().UnixNano())
	// log.SetLevel(log.LEVEL_DEBUG)
	log.SetLevel(log.LEVEL_INFO)
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

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes() string {
	n := rand.Intn(1000)
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
func TestServer(t *testing.T) {
	log.Debugf("Enter TestServer..")
	numTimeout := 0
	numUnmatch := 0
	numSuccess := 0
	for i := 0; i < 10000; i++ {
		val := RandStringBytes()
		log.Infof("loop: %d. msg len: %d", i, len(val))
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
