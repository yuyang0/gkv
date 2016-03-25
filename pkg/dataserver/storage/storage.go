package storage

import (
	"encoding/binary"
	"fmt"
)

type IKVStorage interface {
	Get(key string) (val []byte, err error)
	Set(key string, val []byte) (err error)
	GetUint64(key string) (val uint64, err error)
	SetUint64(key string, val uint64) error
	// MGetUint64(keys [][]byte) (vals []uint64, err error)
	// MSetUint64(keys [][]byte, vals []uint64) (err error)
	Merge(key string, n uint64) (err error)

	Delete(key string) error
	Close() error
}

const (
	UINT8_MAX  = uint64(^uint8(0))
	UINT16_MAX = uint64(^uint16(0))
	UINT32_MAX = uint64(^uint32(0))
	UINT64_MAX = uint64(^uint64(0))
)

func BytesToUint64(value []byte) (uint64, error) {
	var val uint64
	if len(value) == 0 {
		val = 0
	} else if len(value) == 1 {
		val = uint64(value[0])
	} else if len(value) == 2 {
		val = uint64(binary.LittleEndian.Uint16(value))
	} else if len(value) == 4 {
		val = uint64(binary.LittleEndian.Uint32(value))
	} else if len(value) == 8 {
		val = uint64(binary.LittleEndian.Uint64(value))
	} else {
		err := fmt.Errorf("value is not a uint64")
		return 0, err
	}
	return val, nil
}

func Uint64ToBytes(val uint64) []byte {
	var bytes []byte
	if val <= UINT8_MAX {
		bytes = make([]byte, 1)
		bytes[0] = uint8(val)
	} else if val <= UINT16_MAX {
		bytes = make([]byte, 2)
		binary.LittleEndian.PutUint16(bytes, uint16(val))
	} else if val <= UINT32_MAX {
		bytes = make([]byte, 4)
		binary.LittleEndian.PutUint32(bytes, uint32(val))
	} else {
		bytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, uint64(val))
	}
	return bytes
}
