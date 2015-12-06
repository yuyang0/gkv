package common

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/yuyang0/gkv/pkg/utils/log"
)

type TcpClient struct {
	mu         sync.Mutex
	connMap    map[string]*Connection
	sessionMap map[int]chan *Msg
	respChan   chan *Msg
}

func NewTcpClient() *TcpClient {
	client := &TcpClient{
		connMap: make(map[string]*Connection, 10),
	}
	go func() {
		for msg := range client.respChan {
			sessionId := msg.sessionId

			client.sessionMap[sessionId] <- msg
		}
	}()
	//this goroutine used to check the idle connections
	// when this connection stays idle for 15 minutes, we will close it.
	go func() {
		for {
			client.mu.Lock()
			for addr, conn := range client.connMap {
				if time.Since(conn.lastUseTime).Minutes() > 15 {
					delete(client.connMap, addr)
				}
			}
			client.mu.Unlock()
			time.Sleep(5 * time.Minute)
		}
	}()
	return client
}

func (client *TcpClient) SendMsg(host string, port int, msg *Msg) bool {
	addr := host + ":" + string(port)

	client.mu.Lock()

	connection, ok := client.connMap[addr]
	if ok {
		go connection.SendMsg(msg)
	} else {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.WarnErrorf(err, "Can't dail to %s", addr)
			return false
		}
		connection := NewConnection(conn)
		client.connMap[addr] = connection
		go connection.SendMsg(msg)
	}
	client.sessionMap[msg.sessionId] = make(chan *Msg, 1)

	client.mu.Unlock()
	return true
}

func (client *TcpClient) SafeDeleteSesion(sessionId int) {
	client.mu.Lock()
	delete(client.sessionMap, sessionId)
	client.mu.Unlock()
}

func (client *TcpClient) GetRespBlock(sessionId int) (*Msg, error) {
	session_chan, ok := client.sessionMap[sessionId]
	if !ok {
		log.Errorf("Can't get session channel(%d)", sessionId)
		return nil, fmt.Errorf("Can't get session channel(%d)", sessionId)
	}

	msg := <-session_chan
	client.SafeDeleteSesion(sessionId)
	return msg, nil
}

func (client *TcpClient) GetRespNonBlock(sessionId int) *Msg {
	session_chan, ok := client.sessionMap[sessionId]
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
