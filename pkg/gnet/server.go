package gnet

import (
	"net"
	"sync"
	"time"

	"github.com/yuyang0/gkv/pkg/utils/log"
)

type TcpServer struct {
	listenAddr string
	listener   net.Listener
	connMap    map[string]*Connection
	reqChan    chan *Msg
	stopChan   chan bool

	mu sync.Mutex
}

func NewTcpServer(addr string) *TcpServer {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.PanicErrorf(err, "Listen Error.")
		return nil
	}
	server := &TcpServer{
		listenAddr: addr,
		listener:   listener,
		connMap:    make(map[string]*Connection),
		reqChan:    make(chan *Msg, 100), // all request message will write to this chan

		stopChan: make(chan bool, 1),
	}
	//this goroutine used to check the idle connections
	// when this connection stays idle for 15 minutes, we will close it.
	go func() {
		for {
			server.mu.Lock()
			for addr, conn := range server.connMap {
				if time.Since(conn.LastUseTime()).Minutes() > 15 {
					delete(server.connMap, addr)
					conn.Disconnect()
				}
			}
			server.mu.Unlock()
			time.Sleep(5 * time.Minute)
		}
	}()
	return server
}

//TODO:
func (server *TcpServer) SendResp(msg *Msg) bool {
	return true
}

func (server *TcpServer) Start() {
	for {
		// check if need to stop the loop..
		select {
		case <-server.stopChan:
			server.mu.Lock()
			for addr, conn := range server.connMap {
				delete(server.connMap, addr)
				conn.Disconnect()
			}
			server.mu.Unlock()
		default:

		}

		conn, err := server.listener.Accept()
		if err != nil {
			log.WarnErrorf(err, "Accept Error.")
			return
		}
		log.Infof("Accept connection from %s", conn.RemoteAddr().String())
		go func(conn net.Conn) {
			connection := NewConnection(conn, true, -1)

			server.safeAddConn(connection)

			for {
				msg := connection.ReceiveMsg()
				if msg == nil {
					server.safeDeleteConn(connection.addr)
					connection.Disconnect()
					return
				}
				msg.SetConnection(connection)
				// log.Debugf("[Server] Get msg: %s", msg.String())
				server.reqChan <- msg
			}
		}(conn)
	}
}

func (s *TcpServer) Stop() {
	s.stopChan <- true
	s.listener.Close()
	return
}

func (server *TcpServer) safeAddConn(conn *Connection) {
	addr := conn.addr
	server.mu.Lock()
	server.connMap[addr] = conn
	server.mu.Unlock()
}

func (server *TcpServer) safeDeleteConn(addr string) {
	server.mu.Lock()
	delete(server.connMap, addr)
	server.mu.Unlock()
}
