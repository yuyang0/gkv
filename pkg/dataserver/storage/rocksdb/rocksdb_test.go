package storage

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"bitbucket.org/gt-dev/gt-apm/server/utils"
)

var sto IKVStorage = nil
var jsonSto IJsonStorage = nil

func initStorage() {
	if sto == nil && jsonSto == nil {
		rand.Seed(time.Now().UTC().UnixNano())

		var err error
		sto, err = NewRocksdbKVStorage("./tmpdb")
		if err != nil {
			panic("can't open kv rocksdb.")
		}
		jsonSto, err = NewRocksdbJsonStorage("./tmpjsondb")
		if err != nil {
			panic("can't open json rocksdb.")
		}

	}
}

func getJsonRecords() []map[string]string {
	jsonRecords := make([]map[string]string, 0, 100)

	existStr := make(map[string]bool)

	getUniqueStr := func() string {
		ss := ""
		for ss == "" {
			ss = utils.RandString()
			if _, ok := existStr[ss]; ok {
				ss = ""
			}
		}
		existStr[ss] = true
		return ss
	}

	for i := 0; i < 100; i++ {
		jsonRecords = append(jsonRecords, map[string]string{
			"key": getUniqueStr(),
			"val": utils.RandString(),
		})
	}
	return jsonRecords
}

func TestBytes(t *testing.T) {
	initStorage()

	mappings := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := utils.RandString()
		val := utils.RandString()
		mappings[key] = val
		err := sto.Set(string(key), []byte(val))
		if err != nil {
			t.Fatalf("can't put new entry to rocksdb(%v %v)", key, val)
		}
	}
	success := 0
	fail := 0
	for k, v := range mappings {
		val, err := sto.Get(string(k))
		if err != nil {
			t.Fatalf("Can't get (%v)", k)
		}
		if string(val) == v {
			success++
		} else {
			t.Logf("Fail: %v %v %v\n", k, v, string(val))
			fail++
		}
	}
	if fail > 0 {
		t.Errorf("success: %v, fail: %v\n", success, fail)
	}
}

func TestUint64(t *testing.T) {
	initStorage()

	mappings := make(map[string]uint64)
	for i := 0; i < 100; i++ {
		key := utils.RandString()
		if len(key) == 0 {
			continue
		}
		if _, ok := mappings[key]; ok {
			continue
		}
		val := uint64(rand.Intn(1000))
		mappings[key] = val
		err := sto.SetUint64(string(key), val)
		if err != nil {
			t.Fatalf("can't put new entry to rocksdb(%v %v)", key, val)
		}
	}
	success := 0
	fail := 0
	for k, v := range mappings {
		val, err := sto.GetUint64(string(k))
		if err != nil {
			t.Fatalf("Can't get (%v)", k)
		}
		if val == v {
			success++
		} else {
			t.Logf("Fail: %v %v\n", v, val)
			fail++
		}
	}
	if fail > 0 {
		t.Errorf("success: %v, fail: %v\n", success, fail)
	}
}

func TestAddMerge(t *testing.T) {
	initStorage()

	mappings := make(map[string]uint64)
	for i := 0; i < 100; i++ {
		key := utils.RandString()
		if key == "" {
			continue
		}
		key = "ADD:" + key
		if _, ok := mappings[key]; ok {
			continue
		}
		val := uint64(rand.Intn(1000))
		mappings[key] = val
		sto.Delete(key)
		err := sto.Merge(string(key), val)
		// vvv, _ := sto.GetUint64(string(key))
		// t.Logf("key: %v, val:  %v, correct: %v\n", key, vvv, val)

		if err != nil {
			t.Fatalf("can't put new entry to rocksdb(%v %v)", key, val)
		}
	}
	t.Logf("total length: %v\n", len(mappings))

	for k, v := range mappings {
		err := sto.Merge(string(k), v)
		if err != nil {
			t.Fatalf("can't Merge")
		}
	}

	for k, v := range mappings {
		err := sto.Merge(string(k), v)
		if err != nil {
			t.Fatalf("can't Merge")
		}
	}

	success := 0
	fail := 0
	for k, v := range mappings {
		val, err := sto.GetUint64(string(k))
		if err != nil {
			t.Fatalf("Can't get (%v)", k)
		}
		if val == 3*v {
			success++
		} else {
			t.Logf("Fail: %v %v\n", v, val)
			fail++
		}
	}
	if fail > 0 {
		t.Errorf("success: %v, fail: %v\n", success, fail)
	}
}

func TestMaxMerge(t *testing.T) {
	initStorage()

	mappings := make(map[string]uint64)
	for i := 0; i < 100; i++ {
		key := utils.RandString()
		if key == "" {
			continue
		}
		key = "MAX:" + key
		if _, ok := mappings[key]; ok {
			continue
		}
		val := uint64(rand.Intn(1000))
		mappings[key] = val
		sto.Delete(key)
		err := sto.Merge(string(key), val)

		if err != nil {
			t.Fatalf("can't put new entry to rocksdb(%v %v)", key, val)
		}
	}
	t.Logf("total length: %v\n", len(mappings))

	for k, v := range mappings {
		val := uint64(rand.Intn(1000))
		if val > v {
			mappings[k] = val
		}
		err := sto.Merge(string(k), val)
		if err != nil {
			t.Fatalf("can't Merge")
		}
	}

	for k, v := range mappings {
		val := uint64(rand.Intn(1000))
		if val > v {
			mappings[k] = val
		}
		err := sto.Merge(string(k), val)
		if err != nil {
			t.Fatalf("can't Merge")
		}
	}

	success := 0
	fail := 0
	for k, v := range mappings {
		val, err := sto.GetUint64(string(k))
		if err != nil {
			t.Fatalf("Can't get (%v)", k)
		}
		if val == v {
			success++
		} else {
			t.Logf("Fail: %v %v\n", v, val)
			fail++
		}
	}
	if fail > 0 {
		t.Errorf("success: %v, fail: %v\n", success, fail)
	}
}

// test for json storage
func TestDocGetSet(t *testing.T) {
	initStorage()

	jsonRecords := getJsonRecords()

	for _, v := range jsonRecords {
		key := v["key"]
		jsonSto.DocSet(key, v)
	}
	fails := 0
	success := 0
	for _, v := range jsonRecords {
		key := v["key"]
		val, _ := jsonSto.FieldGet(key, "val")
		realVal := val.(string)
		if realVal != v["val"] {
			fails++
		} else {
			success++
		}
	}

	if fails > 0 {
		t.Errorf("sucess: %v, fails: %v", success, fails)
	}
}

func TestFieldGetSet(t *testing.T) {
	initStorage()

	jsonRecords := getJsonRecords()

	for _, v := range jsonRecords {
		key := v["key"]
		val := utils.RandString()
		jsonSto.FieldSet(key, "val", val)
		v["val"] = val
	}
	fails := 0
	success := 0
	for _, v := range jsonRecords {
		key := v["key"]
		val, _ := jsonSto.FieldGet(key, "val")
		realVal := val.(string)
		if realVal != v["val"] {
			fails++
		} else {
			success++
		}
	}
	if fails > 0 {
		t.Errorf("sucess: %v, fails: %v", success, fails)
	}
}

func sliceEqual(a []interface{}, b []string) bool {
	if a == nil && b == nil {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func TestArrayAppend(t *testing.T) {
	initStorage()

	records := make(map[string][]string, 100)
	for i := 0; i < 100; i++ {
		key := utils.RandString()
		jsonSto.DocDelete(key)

		val := []string{utils.RandString(), utils.RandString()}
		records[key] = val
		for _, v := range val {
			jsonSto.ArrayAppend(key, "", v, true)
		}
	}

	success := 0
	fails := 0
	for k, v := range records {
		rawval, _ := jsonSto.DocGet(k)
		// realVal := rawval.([]string)
		if sliceEqual(rawval.([]interface{}), v) == false {
			fails++
		} else {
			success++
		}
	}
	if fails > 0 {
		t.Errorf("[TestArrayAppend] sucess: %v, fails: %v", success, fails)
	}
}

func TestFieldInc(t *testing.T) {
	initStorage()

	records := make(map[string]int, 100)
	for i := 0; i < 100; i++ {
		key := utils.RandString()
		if strings.Trim(key, " ") == "" {
			continue
		}
		if _, ok := records[key]; ok {
			continue
		}
		jsonSto.DocDelete(key)

		val := 0
		for i := 0; i < 50; i++ {
			v := rand.Intn(100)
			val += v
			jsonSto.FieldInc(key, key, uint64(v))
		}
		records[key] = val
	}

	success := 0
	fails := 0
	for k, v := range records {
		rawval, _ := jsonSto.FieldGet(k, k)
		val, _ := rawval.(uint64)
		if val == uint64(v) {
			fails++
		} else {
			success++
		}
	}
	if fails > 0 {
		t.Errorf("[TestFieldInc] sucess: %v, fails: %v", success, fails)
	}
}
