package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	// "github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/yuyang0/gkv/pkg/slave/storage"
)

type LevelDB struct {
	db *leveldb.DB
}

func NewLevelDB(dataDir string) *LevelDB {
	db, err := leveldb.OpenFile(dataDir, nil)
	if err != nil {
		return nil
	}
	return &LevelDB{
		db: db,
	}
}

func (db *LevelDB) Get(key []byte, options interface{}) (val []byte, err error) {
	val, err = db.db.Get(key, nil)
	return val, err
}

func (db *LevelDB) Put(key, val []byte, options interface{}) error {
	err := db.db.Put(key, val, nil)
	return err
}

func (db *LevelDB) Delete(key []byte, options interface{}) error {
	err := db.db.Delete(key, nil)
	return err
}

func (db *LevelDB) Close() error {
	return db.db.Close()
}

func NewStorage(dir string) storage.Storage {
	return NewLevelDB(dir)
}
