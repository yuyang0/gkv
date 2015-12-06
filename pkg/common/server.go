package common

import (
	"net"

	"github.com/yuyang0/gkv/pkg/utils/log"
)

type TcpServer struct {
	host      string
	port      int
	listener  net.Listener
	conn_map  map[string]*Connection
	conn_chan chan *Msg
}

func NewTcpServer(host string, port int) *TcpServer {
	addr := host + ":" + string(port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.PanicErrorf(err, "Listen Error.")
		return nil
	}
	server := &TcpServer{
		host:      host,
		port:      port,
		listener:  listener,
		conn_map:  make(map[string]*Connection),
		conn_chan: make(chan *Msg),
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
			server.conn_map[addr] = info

			for {
				msg := <-info.incoming
				go HandleMsg(info, msg)
			}
		}(conn)
	}
}

func HandleMsg(conn *Connection, msg *Msg) {
}
