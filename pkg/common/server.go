package common

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
	}
	//this goroutine used to check the idle connections
	// when this connection stays idle for 15 minutes, we will close it.
	go func() {
		for {
			server.mu.Lock()
			for addr, conn := range server.connMap {
				if time.Since(conn.lastUseTime).Minutes() > 15 {
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

func (server *TcpServer) Loop() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			log.WarnErrorf(err, "Accept Error.")
			continue
		}
		go func(conn net.Conn) {
			connection := NewConnection(conn)

			server.safeAddConn(connection)

			for {
				msg := connection.ReceiveMsg()
				if msg == nil {
					server.safeDeleteConn(connection.addr)
					connection.Disconnect()
					return
				}
				msg.SetConnection(connection)
				server.reqChan <- msg
			}
		}(conn)
	}
}

func (s *TcpServer) Stop() {
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
