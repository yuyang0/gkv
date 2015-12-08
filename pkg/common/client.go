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
	if ok {
		go connection.SendMsg(msg)
	} else {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.WarnErrorf(err, "Can't dail to %s", addr)
			return false
		}
		connection := NewConnection(conn)
		client.conn_map[addr] = connection
		go connection.SendMsg(msg)
	}
	client.session_map[msg.sessionId] = make(chan *Msg, 1)

	client.mu.Unlock()
	return true
}

func (client *TcpClient) SafeDeleteSesion(sessionId int) {
	client.mu.Lock()
	delete(client.session_map, sessionId)
	client.mu.Unlock()
}

func (client *TcpClient) GetRespBlock(sessionId int) (*Msg, error) {
	session_chan, ok := client.session_map[sessionId]
	if !ok {
		log.Errorf("Can't get session channel(%d)", sessionId)
		return nil, fmt.Errorf("Can't get session channel(%d)", sessionId)
	}

	msg := <-session_chan
	client.SafeDeleteSesion(sessionId)
	return msg, nil
}

func (client *TcpClient) GetRespNonBlock(sessionId int) *Msg {
	session_chan, ok := client.session_map[sessionId]
	if !ok {
		log.Errorf("Can't get session channel(%d)", sessionId)
		return nil
	}

	select {
	case msg := <-session_chan:
		client.SafeDeleteSesion(sessionId)
		return msg
	default:
		return nil
	}
}
