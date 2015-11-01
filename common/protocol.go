package common

import (
	"fmt"
)

type RequestPacket struct {
	length         int
	endcoding_type int
	data           []byte
}

type ResponsePacket struct {
	length        int
	encoding_type int
	data          []byte
}

func ParseReqFromString(data []byte) *RequestPacket {

}

func (self *RequestPacket) ConvertReqToString() []byte {

}

func ParseRespFromString(data []string) *ResponsePacket {

}

func (self *ResponsePacket) ConvertRespToString() []byte {

}
