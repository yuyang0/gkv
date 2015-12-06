package common

import (
	"bufio"
	"fmt"
	"io"
	"strconv"

	"github.com/yuyang0/gkv/pkg/utils/log"
)

type MsgHeader struct {
	length     int
	encodeType int
	requestId  int
	responseTo int
	pCode      int
}

type Msg struct {
	MsgHeader
	data []byte
}

func ReadMsg(reader *bufio.Reader) *Msg {
	line, err := reader.ReadString('\n')
	if err != nil {
		log.WarnErrorf(err, "can't read message length.")
		return nil
	}
	length, err := strconv.Atoi(line[:len(line)-2])
	if err != nil {
		log.WarnErrorf(err, "Can't convert message length to integer.")
		return nil
	}
	line, err = reader.ReadString('\n')
	if err != nil {
		log.WarnErrorf(err, "Can't read message encode type..")
		return nil
	}
	encodeType, err := strconv.Atoi(line[:len(line)-2])

	line, err = reader.ReadString('\n')
	requestId, err := strconv.Atoi(line[:len(line)-2])

	line, err = reader.ReadString('\n')
	responseTo, err := strconv.Atoi(line[:len(line)-2])

	line, err = reader.ReadString('\n')
	pCode, err := strconv.Atoi(line[:len(line)-2])
	// ignore the rest headers
	for line == "\r\n" {
		line, err = reader.ReadString('\n')
	}
	data := make([]byte, length)
	n, err := io.ReadFull(reader, data)
	if err != nil {
	}
	return &Msg{
		length, encodeType, requestId, responseTo, pCode, data}
}

func (self *Msg) ConvertToBytes() []byte {
	ss := fmt.Sprintf("%d\r\n%d\r\n%d\r\n%d\r\n%d\r\n\r\n%s",
		self.length, self.encodeType, self.requestId, self.responseTo, self.pCode, self.data)
	return []byte(ss)
}
