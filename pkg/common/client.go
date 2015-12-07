package common

import (
	"fmt"
	"github.com/yuyang0/gkv/pkg/utils/log"
	"net"
	"sync"
)

type TcpClient struct {
	mu          sync.Mutex
	conn_map    map[string]*Connection
	session_map map[int]chan *Msg
	resp_chan   chan *Msg
}

func NewTcpClient() *TcpClient {
	client := &TcpClient{
		conn_map: make(map[string]*Connection, 10),
	}
	go func() {
		for msg := range client.resp_chan {
			sessionId := msg.sessionId

			client.session_map[sessionId] <- msg
		}
	}()
	return client
}

func (client *TcpClient) SendMsg(host string, port int, msg *Msg) bool {
	addr := host + ":" + string(port)
	client.mu.Lock()

	connection, ok := client.conn_map[addr]
	if !ok {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.WarnErrorf(err, "Can't dail to %s", addr)
			return false
		}
		connection := NewConnection(conn)
		client.conn_map[addr] = connection
	}
	go connection.WriteMsgToChan(msg)
	client.session_map[msg.sessionId] = make(chan *Msg, 1)

	client.mu.Unlock()
	return true
}

func (client *TcpClient) WaitForResp(sessionId int) (*Msg, error) {
	session_chan, ok := client.session_map[sessionId]
	if !ok {
		log.Errorf("Can't get session channel(%d)", sessionId)
		return nil, fmt.Errorf("Can't get session channel(%d)", sessionId)
	}

	client.mu.Lock()
	delete(client.session_map, sessionId)
	client.mu.Unlock()

	msg := <-session_chan
	return msg, nil
}
