package storage

// import (
// 	"github.com/syndtr/goleveldb/leveldb"
// )

type Storage interface {
	Get(key []byte, options interface{}) (val []byte, err error)
	Put(key []byte, val []byte, options interface{}) error
	Delete(key []byte, options interface{}) error
	Close() error
}
