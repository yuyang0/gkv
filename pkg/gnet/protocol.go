package gnet

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/adler32"
	"io"
	"math/rand"

	"github.com/yuyang0/gkv/pkg/utils/log"
)

type EncodeType uint32

const (
	MAGIC_STR     = "\r\r\r\r"
	LEN_MAGIC_STR = 4
)

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
	length     uint32
	encodeType EncodeType
	sessionId  uint32
	pCode      uint32

	data []byte

	connection *Connection
}

func NewTimeoutMsg(sessionId uint32) *Msg {
	return &Msg{
		length:     0,
		encodeType: ENCODE_TYPE_JSON,
		sessionId:  sessionId,
		pCode:      MSG_TIMEOUT,
	}
}

func NewErrorMsg(sessionId uint32) *Msg {
	return &Msg{
		length:     0,
		encodeType: ENCODE_TYPE_JSON,
		sessionId:  sessionId,
		pCode:      MSG_ERROR,
	}
}

func readMsgFromReader(reader *bufio.Reader) (*Msg, error) {
begin:
	// first we need to ignore the magic bytes
	for {
		bytes, err := reader.Peek(LEN_MAGIC_STR)
		if err != nil {
			log.WarnErrorf(err, "Can't peek from reader..")
			return nil, err
		}
		if string(bytes) == MAGIC_STR {
			reader.Discard(LEN_MAGIC_STR)
			break
		} else {
			reader.Discard(1)
		}
	}
	tmp := make([]byte, 4)
	_, err := reader.Read(tmp)
	if err != nil {
		log.WarnErrorf(err, "can't read message length.")
		return nil, err
	}
	length := binary.BigEndian.Uint32(tmp)
	_, err = reader.Read(tmp)
	if err != nil {
		log.WarnErrorf(err, "Can't read message encode type..")
		return nil, err
	}
	encodeType := binary.BigEndian.Uint32(tmp)

	_, err = reader.Read(tmp)
	if err != nil {
		log.WarnErrorf(err, "Can't read channel id.")
		return nil, err
	}
	sessionId := binary.BigEndian.Uint32(tmp)

	_, err = reader.Read(tmp)
	if err != nil {
		log.WarnErrorf(err, "Can't read channel id.")
		return nil, err
	}
	pCode := binary.BigEndian.Uint32(tmp)

	data := make([]byte, length)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		log.WarnErrorf(err, "Can't read data..")
		return nil, err
	}

	_, err = reader.Read(tmp)
	if err != nil {
		log.WarnErrorf(err, "Can't read checkSum")
		return nil, err
	}
	checkSum := binary.BigEndian.Uint32(tmp)
	if checkSum != adler32.Checksum(data) {
		log.Warnf("checkSum is incorrect.so we will ignore this message.")
		goto begin
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

func (msg *Msg) ConvertToBytes() []byte {
	getBytes := func(val uint32) []byte {
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, val)
		return tmp
	}
	checkSum := adler32.Checksum(msg.data)

	var b bytes.Buffer
	b.Write([]byte(MAGIC_STR))
	b.Write(getBytes(msg.length))
	b.Write(getBytes(uint32(msg.encodeType)))
	b.Write(getBytes(msg.sessionId))
	b.Write(getBytes(msg.pCode))
	b.Write(msg.data)
	b.Write(getBytes(checkSum))
	return b.Bytes()
}

func NewReqMsg(encodeType EncodeType, pCode uint32, data []byte) *Msg {
	length := len(data)
	return &Msg{
		length:     uint32(length),
		encodeType: encodeType,
		sessionId:  genSessionId(),
		pCode:      pCode,
		data:       data,
	}
}

func NewRespMsg(encodeType EncodeType, sessionId uint32, pCode uint32, data []byte) *Msg {
	length := len(data)
	return &Msg{
		length:     uint32(length),
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
	ss := fmt.Sprintf("{length: %d, sessionId %d, data %s}", msg.length, msg.sessionId, string(msg.data))
	return ss
}

func genSessionId() uint32 {
	return rand.Uint32()
}
