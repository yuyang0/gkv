package packet

type PacketInterface interface {
	encode()
	decode()
}
