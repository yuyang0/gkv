package gnet

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/yuyang0/gkv/pkg/utils/log"
)

type Session struct {
	sessionId   int
	sessionChan chan *Msg
	createdTime time.Time
}

func NewSession(sessionId int) *Session {
	return &Session{
		sessionId:   sessionId,
		sessionChan: make(chan *Msg, 1),
		createdTime: time.Now(),
	}
}

type TcpClient struct {
	mu         sync.Mutex
	connMap    map[string]*Connection
	sessionMtx sync.RWMutex
	sessionMap map[int]*Session

	respChan chan *Msg
}

func NewTcpClient() *TcpClient {
	client := &TcpClient{
		connMap:    make(map[string]*Connection, 10),
		sessionMap: make(map[int]*Session, 10),
		respChan:   make(chan *Msg, 100),
	}
	//this goroutine used to check the idle connections
	// when this connection stays idle for 15 minutes, we will close it.
	go func() {
		for {
			client.mu.Lock()
			for addr, conn := range client.connMap {
				if time.Since(conn.lastUseTime).Minutes() > 15 {
					delete(client.connMap, addr)
					conn.Disconnect()
				}
			}
			client.mu.Unlock()
			time.Sleep(5 * time.Minute)
		}
	}()
	// this goroutine used to move response to the session channel
	go func() {
		for msg := range client.respChan {
			sessionId := msg.sessionId

			log.Debugf("[client] Move resp(%d) to session", sessionId)
			client.sessionMtx.RLock()
			session, ok := client.sessionMap[sessionId]
			if ok {
				if len(session.sessionChan) == 1 {
					log.Infof("session(%d) is timeout..")
				} else {
					select {
					case session.sessionChan <- msg:
						log.Infof("Write reponse for session(%d) to channel", sessionId)
					default:
						log.Errorf("BUG: write to session(%d) channel should not block.", sessionId)
					}
				}
			} else {
				log.Warnf("A bug or a wrong message, because we can't find session(%d)", sessionId)
			}
			client.sessionMtx.RUnlock()
		}
	}()

	//this goroutine used to check the timeout session
	go func(c *TcpClient) {
		for {
			c.sessionMtx.RLock()
			for sessionId, session := range c.sessionMap {
				if len(session.sessionChan) >= 1 {
					continue
				}
				if time.Since(session.createdTime) > 120*time.Second {
					select {
					case session.sessionChan <- NewTimeoutMsg(sessionId):
					default:
						log.Errorf("Write timeout session to session(%d) channel should not block..", sessionId)
					}
				}
			}
			c.sessionMtx.RUnlock()

			time.Sleep(5 * time.Second)
		}
	}(client)

	return client
}

func (client *TcpClient) SendReq(addr string, msg *Msg) bool {
	client.mu.Lock()

	connection, ok := client.connMap[addr]
	if ok {
		go connection.SendMsg(msg)
	} else {
		connection = client.createConnection(addr)
		if connection == nil {
			client.mu.Unlock()
			return false
		}
		go connection.SendMsg(msg)
	}
	client.mu.Unlock()

	client.safeAddSession(msg.sessionId)
	return true
}

func (client *TcpClient) GetRespBlock(sessionId int) (*Msg, error) {
	client.sessionMtx.RLock()
	session, ok := client.sessionMap[sessionId]
	client.sessionMtx.RUnlock()

	if !ok {
		log.Errorf("Can't get session channel(%d)", sessionId)
		return nil, fmt.Errorf("Can't get session channel(%d)", sessionId)
	}

	msg := <-session.sessionChan
	client.safeDeleteSession(sessionId)
	return msg, nil
}

// Get reponse of a session without block..
func (client *TcpClient) GetRespNonBlock(sessionId int) *Msg {
	var msg *Msg

	client.sessionMtx.RLock()
	session, ok := client.sessionMap[sessionId]
	client.sessionMtx.RUnlock()

	if ok {
		select {
		case msg = <-session.sessionChan:
			client.safeDeleteSession(sessionId)
		default:
			log.Errorf("BUG: the seesonChan(%d) in finishedSessionMap should not block.", sessionId)
			msg = nil
		}
	} else {
		log.Errorf("Can't get session channel(%d)", sessionId)
		msg = nil
	}
	return msg

}

// this function don't need lock, we will lock in the caller
func (client *TcpClient) createConnection(addr string) *Connection {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.WarnErrorf(err, "Can't dail to %s", addr)
		return nil
	}
	connection := NewConnection(conn)
	client.connMap[addr] = connection
	go func(c *Connection) {
		for {
			msg := c.ReceiveMsg()
			if msg == nil {
				c.Disconnect()
				return
			}
			// log.Debugf("[client] receive msg(%s)", msg.String())
			client.respChan <- msg
			log.Debugf("[client] finished write msg(%d) to respChan", msg.sessionId)
		}
	}(connection)

	return connection
}

func (client *TcpClient) safeAddSession(sessionId int) {
	client.sessionMtx.Lock()
	session := NewSession(sessionId)
	client.sessionMap[sessionId] = session
	client.sessionMtx.Unlock()
}

func (c *TcpClient) safeDeleteSession(sessionId int) {
	c.sessionMtx.Lock()
	session, ok := c.sessionMap[sessionId]
	if ok {
		delete(c.sessionMap, sessionId)
		close(session.sessionChan)
	}
	c.sessionMtx.Unlock()
}

// func (client *TcpClient) safeAddConn(conn *Connection) {
// 	addr := conn.addr
// 	client.mu.Lock()
// 	client.connMap[addr] = conn
// 	client.mu.Unlock()
// }

// func (client *TcpClient) safeDeleteConn(addr string) {
// 	client.mu.Lock()
// 	delete(client.connMap, addr)
// 	client.mu.Unlock()
// }
