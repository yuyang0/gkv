package gnet

type PacketInterface interface {
	encode()
	decode()
}
