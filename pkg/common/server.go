package common

import (
	"net"
	"sync"

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
			addr := conn.RemoteAddr().String()
			info := NewConnection(conn)

			server.mu.Lock()
			server.connMap[addr] = info
			server.mu.Unlock()

			for {
				msg := <-info.incoming
				msg.SetConnection(info)
				server.reqChan <- msg
			}
		}(conn)
	}
}
