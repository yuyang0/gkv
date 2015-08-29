package common

type PacketInterface interface {
	encode()
	decode()
}
