package rocksdb

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/tecbot/gorocksdb"
)

type RocksdbStorage struct {
	dbfname string
	dbtype  string
	db      *gorocksdb.DB
	ro      *gorocksdb.ReadOptions
	wo      *gorocksdb.WriteOptions
	mu      sync.Mutex
}

func NewRocksdbStorage(dbfname string, dbtype string, mergeOp gorocksdb.MergeOperator) (*RocksdbStorage, error) {
	var sto *RocksdbStorage

	if dbtype != "kv" && dbtype != "json" {
		return sto, fmt.Errorf("Unkown db type")
	}

	opts := gorocksdb.NewDefaultOptions()

	if mergeOp != nil {
		opts.SetMergeOperator(mergeOp)
	}
	// opts.IncreaseParallelism(runtime.NumCPU())
	// opts.OptimizeLevelStyleCompaction(0)
	opts.SetCreateIfMissing(true)

	db, err := gorocksdb.OpenDb(opts, dbfname)
	if err != nil {
		return sto, err
	}
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()

	sto = &RocksdbStorage{
		dbfname: dbfname,
		db:      db,
		ro:      ro,
		wo:      wo,
	}
	return sto, nil
}

func NewRocksdbKVStorage(dbfname string) (*RocksdbStorage, error) {
	var mergeOp gorocksdb.MergeOperator = &Uint64MergeOperator{}
	return NewRocksdbStorage(dbfname, "kv", mergeOp)
}

func NewRocksdbJsonStorage(dbfname string) (*RocksdbStorage, error) {
	var mergeOp gorocksdb.MergeOperator = &JsonMergeOperator{}
	return NewRocksdbStorage(dbfname, "json", mergeOp)
}

//when the key not exists, return nil, nil
func (sto *RocksdbStorage) Get(key string) (val []byte, err error) {
	db := sto.db
	ro := sto.ro
	val, err = db.GetBytes(ro, []byte(key))
	return val, err
}

func (sto *RocksdbStorage) Set(key string, val []byte) error {
	db := sto.db
	wo := sto.wo

	err := db.Put(wo, []byte(key), val)
	if err != nil {
		return err
	}
	return nil
}

// when the key not exist, return 0, nil
func (sto *RocksdbStorage) GetUint64(key string) (uint64, error) {
	bytes, err := sto.Get(key)
	if err != nil {
		return 0, err
	}
	val, err := BytesToUint64(bytes)
	return val, err
}

func (sto *RocksdbStorage) SetUint64(key string, val uint64) error {
	bytes := Uint64ToBytes(val)
	err := sto.Set(key, bytes)
	return err
}

// func MGetUint64(keys [][]byte) (vals []uint64, err error) {

// }

// func MSetUint64(keys [][]byte, vals []uint64) (err error) {

// }

func (sto *RocksdbStorage) Merge(key string, v uint64) (err error) {
	wo := sto.wo
	db := sto.db

	err = db.Merge(wo, []byte(key), Uint64ToBytes(v))
	return err
}

func (sto *RocksdbStorage) Delete(key string) error {
	wo := sto.wo
	db := sto.db
	return db.Delete(wo, []byte(key))
}

func (sto *RocksdbStorage) Close() (err error) {
	sto.db.Close()
	return nil
}

// method for json storage
func (sto *RocksdbStorage) docGet1(key string, needLock bool) (interface{}, error) {
	if needLock == true {
		sto.mu.Lock()
		defer sto.mu.Unlock()
	}

	db := sto.db
	ro := sto.ro
	var realVal interface{}

	vbytes, err := db.GetBytes(ro, []byte(key))
	if err != nil {
		return realVal, err
	}
	// the key not exists
	if len(vbytes) == 0 {
		return nil, nil
	}
	err = json.Unmarshal(vbytes, &realVal)
	return realVal, err
}

func (sto *RocksdbStorage) DocGet(key string) (interface{}, error) {
	return sto.docGet1(key, true)
}

func (sto *RocksdbStorage) docSet1(key string, val interface{}, needLock bool) error {
	if needLock == true {
		sto.mu.Lock()
		defer sto.mu.Unlock()
	}
	db := sto.db
	wo := sto.wo

	vbytes, err := json.Marshal(val)
	if err != nil {
		return err
	}
	err = db.Put(wo, []byte(key), vbytes)
	if err != nil {
		return err
	}

	return nil
}

func (sto *RocksdbStorage) DocSet(key string, val interface{}) error {
	return sto.docSet1(key, val, true)
}

func (sto *RocksdbStorage) DocDelete(key string) error {
	return sto.Delete(key)
}

func (sto *RocksdbStorage) FieldGet(key string, jsonKey string) (interface{}, error) {
	val, err := sto.DocGet(key)
	if err != nil {
		return val, err
	}
	if jsonKey == "" {
		return val, nil
	}
	if val == nil {
		return nil, nil
	}
	if realVal, ok := val.(map[string]interface{}); ok {
		v, ok := realVal[jsonKey]
		if ok == false {
			return nil, nil
		}
		return v, nil
	} else {
		return nil, fmt.Errorf("value of %v is not a map", key)
	}
}

func (sto *RocksdbStorage) FieldSet(key string, jsonKey string, jsonVal interface{}) error {
	sto.mu.Lock()
	defer sto.mu.Unlock()

	var realVal map[string]interface{}

	val, err := sto.docGet1(key, false)
	if err != nil {
		return err
	}
	if val == nil {
		realVal = make(map[string]interface{}, 1)
	} else {
		var ok bool
		realVal, ok = val.(map[string]interface{})
		if ok == false {
			return fmt.Errorf("the value of %v is not a map.", key)
		}
	}
	realVal[jsonKey] = jsonVal
	err = sto.docSet1(key, realVal, false)
	return err
}

func (sto *RocksdbStorage) FieldInc(key string, jsonKey string, v uint64) error {
	sto.mu.Lock()
	defer sto.mu.Unlock()

	var realVal map[string]interface{}

	val, err := sto.docGet1(key, false)
	if err != nil {
		return err
	}
	if val == nil {
		realVal = make(map[string]interface{}, 1)
	} else {
		var ok bool
		realVal, ok = val.(map[string]interface{})
		if ok == false {
			return fmt.Errorf("the value of %v is not a map.", key)
		}
	}
	oldv, _ := realVal[jsonKey].(uint64)
	realVal[jsonKey] = oldv + v
	err = sto.docSet1(key, realVal, false)
	return err
}

func (sto *RocksdbStorage) ArrayAppend(key string, jsonKey string, jsonVal interface{}, unique bool) (err error) {
	valExists := func(arr []interface{}, ele interface{}) bool {
		for _, val := range arr {
			if val == ele {
				return true
			}
		}
		return false
	}
	sto.mu.Lock()
	defer sto.mu.Unlock()

	var arrVal []interface{}
	val, err := sto.docGet1(key, false)
	if err != nil {
		return err
	}
	if jsonKey == "" {
		var docVal []interface{}
		if val == nil {
			docVal = make([]interface{}, 0, 1)
		} else {
			var ok bool
			docVal, ok = val.([]interface{})
			if ok == false {
				return fmt.Errorf("the value of key %v is not []interface{}", key)
			}
		}
		if unique == true && valExists(docVal, jsonVal) == true {
			return nil
		}
		docVal = append(docVal, jsonVal)
		err = sto.docSet1(key, docVal, false)
		return err
	} else {
		var docVal map[string]interface{}
		if val == nil {
			docVal = make(map[string]interface{}, 1)
		} else {
			var ok bool
			docVal, ok = val.(map[string]interface{})
			if ok == false {
				return fmt.Errorf("the value of key %v is not map[string]interface{}", key)
			}
		}
		realVal, ok := docVal[jsonKey]
		if ok == false {
			arrVal = make([]interface{}, 0, 1)
		} else {
			arrVal, ok = realVal.([]interface{})
			if ok == false {
				return fmt.Errorf("the value of (key %v, jsonkey %v) is not map[string]interface{}", key, jsonKey)
			}
		}

		if unique == true && valExists(arrVal, jsonVal) == true {
			return nil
		}
		arrVal = append(arrVal, jsonVal)

		docVal[jsonKey] = arrVal
		err = sto.docSet1(key, docVal, false)
		return err
	}
}
