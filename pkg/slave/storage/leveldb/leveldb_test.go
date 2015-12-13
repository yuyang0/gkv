package leveldb

import (
	"math/rand"
	"testing"
	"time"
)

var (
	db *LevelDB
)

func init() {
	db = NewLevelDB("/tmp/leveldb")
	rand.Seed(time.Now().UnixNano())
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func TestGetSet(t *testing.T) {
	retMap := make(map[string]string, 2000)
	failed := 0
	success := 0
	for i := 0; i < 2000; i++ {
		key := RandStringBytes(32)
		val := RandStringBytes(32)
		retMap[key] = val
		db.Put([]byte(key), []byte(val), nil)
	}
	for k, v := range retMap {
		realK, _ := db.Get([]byte(k), nil)
		if string(realK) != v {
			failed++
		} else {
			success++
		}
	}
	t.Logf("failed: %d", failed)
	t.Logf("success: %d", success)
}
