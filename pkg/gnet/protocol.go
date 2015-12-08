package gnet

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"strconv"

	"github.com/yuyang0/gkv/pkg/utils/log"
)

type EncodeType int

const (
	ENCODE_TYPE_JSON = EncodeType(1 << iota)
	ENCODE_TYPE_PROTOBUF
)

func (t EncodeType) String() string {
	switch t {
	case ENCODE_TYPE_PROTOBUF:
		return "protobuf"
	case ENCODE_TYPE_JSON:
		return "json"
	default:
		return "unkown"
	}
}

const (
	MSG_TIMEOUT = 1
	MSG_ERROR   = 2
)

type Msg struct {
	length     int
	encodeType EncodeType
	sessionId  int
	pCode      int

	data []byte

	connection *Connection
}

func NewTimeoutMsg(sessionId int) *Msg {
	return &Msg{
		length:     0,
		encodeType: ENCODE_TYPE_JSON,
		sessionId:  sessionId,
		pCode:      MSG_TIMEOUT,
	}
}

func NewErrorMsg(sessionId int) *Msg {
	return &Msg{
		length:     0,
		encodeType: ENCODE_TYPE_JSON,
		sessionId:  sessionId,
		pCode:      MSG_ERROR,
	}
}

func readMsgFromReader(reader *bufio.Reader) (*Msg, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		log.WarnErrorf(err, "can't read message length.")
		return nil, err
	}
	length, err := strconv.Atoi(line[:len(line)-2])
	if err != nil {
		log.WarnErrorf(err, "Can't convert message length to integer.")
		return nil, err
	}
	line, err = reader.ReadString('\n')
	if err != nil {
		log.WarnErrorf(err, "Can't read message encode type..")
		return nil, err
	}
	encodeType, err := strconv.Atoi(line[:len(line)-2])
	if err != nil {
		log.WarnErrorf(err, "Can't convert message encodeType to integer.")
		return nil, err
	}
	line, err = reader.ReadString('\n')
	if err != nil {
		log.WarnErrorf(err, "Can't read channel id.")
		return nil, err
	}
	sessionId, err := strconv.Atoi(line[:len(line)-2])
	if err != nil {
		log.WarnErrorf(err, "Can't convert sessionId to integer..")
		return nil, err
	}
	line, err = reader.ReadString('\n')
	pCode, err := strconv.Atoi(line[:len(line)-2])
	// ignore the rest headers
	for {
		line, err = reader.ReadString('\n')
		if line == "\r\n" {
			break
		}
	}
	data := make([]byte, length)
	n, err := io.ReadFull(reader, data)
	if err != nil {
		log.WarnErrorf(err, "Can't read data..")
		return nil, err
	}
	if n != length {
		log.WarnErrorf(err, "Need read %d bytes, but only get %d bytes", length, n)
		return nil, fmt.Errorf("Need read %d bytes, but only get %d bytes", length, n)
	}
	msg := &Msg{
		length:     length,
		encodeType: EncodeType(encodeType),
		sessionId:  sessionId,
		pCode:      pCode,
		data:       data,
	}
	return msg, nil
}

func (self *Msg) ConvertToBytes() []byte {
	ss := fmt.Sprintf("%d\r\n%d\r\n%d\r\n%d\r\n\r\n%s",
		self.length, self.encodeType, self.sessionId, self.pCode, self.data)
	return []byte(ss)
}

func NewReqMsg(encodeType EncodeType, pCode int, data []byte) *Msg {
	length := len(data)
	return &Msg{
		length:     length,
		encodeType: encodeType,
		sessionId:  genSessionId(),
		pCode:      pCode,
		data:       data,
	}
}

func NewRespMsg(encodeType EncodeType, sessionId int, pCode int, data []byte) *Msg {
	length := len(data)
	return &Msg{
		length:     length,
		encodeType: encodeType,
		sessionId:  sessionId,
		pCode:      pCode,
		data:       data,
	}
}

func (msg *Msg) SetConnection(conn *Connection) {
	msg.connection = conn
}

func (msg *Msg) String() string {
	ss := fmt.Sprintf("{\n\tlength: %d \n\tsessionId %d \n\tdata %s}", msg.length, msg.sessionId, string(msg.data))
	return ss
}

func genSessionId() int {
	return rand.Int()
}
