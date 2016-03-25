package rocksdb

import (
	"encoding/json"
	"strings"

	log "github.com/yuyang0/golog"
)

func uint64Add(o1, o2 uint64) uint64 {
	return o1 + o2
}

func uint64Max(o1, o2 uint64) uint64 {
	if o1 > o2 {
		return o1
	} else {
		return o2
	}
}

func uint64Min(o1, o2 uint64) uint64 {
	if o1 < o2 {
		return o1
	} else {
		return o2
	}
}

func getKeyMergeType(fullkey string) string {
	parts := strings.Split(fullkey, ":")
	return parts[0]
}

// uint64 merge operator
type Uint64MergeOperator struct{}

func (op *Uint64MergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	keyStr := string(key)
	mergeType := getKeyMergeType(keyStr)
	var mergeFunc func(o1, o2 uint64) uint64

	switch strings.ToLower(mergeType) {
	case "add":
		mergeFunc = uint64Add
	case "max":
		mergeFunc = uint64Max
	case "min":
		mergeFunc = uint64Min
	default:
		log.Warnf("unkown merge type: %v, key: %v\n", mergeType, string(key))
		return nil, false
	}
	var ret []byte
	val, err := BytesToUint64(existingValue)
	if err != nil {
		log.WarnErrorf(err, "existing value corruption. %v", existingValue)
		val = 0
		// return ret, false
	}
	for _, bytes := range operands {
		tmp, err := BytesToUint64(bytes)
		if err != nil {
			return ret, false
		}
		val = mergeFunc(val, tmp)
	}
	ret = Uint64ToBytes(val)
	return ret, true
}

func (op *Uint64MergeOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	keyStr := string(key)
	mergeType := getKeyMergeType(keyStr)

	var mergeFunc func(o1, o2 uint64) uint64

	switch strings.ToLower(mergeType) {
	case "add":
		mergeFunc = uint64Add
	case "max":
		mergeFunc = uint64Max
	case "min":
		mergeFunc = uint64Min
	default:
		log.Warnf("unkown merge type: %v, key: %v\n", mergeType, string(key))
		return nil, false
	}

	var ret []byte
	o1, err := BytesToUint64(leftOperand)
	if err != nil {
		return ret, false
	}
	o2, err := BytesToUint64(rightOperand)
	if err != nil {
		return ret, false
	}
	newVal := mergeFunc(o1, o2)
	return Uint64ToBytes(newVal), true
}

func (op *Uint64MergeOperator) Name() string {
	return "Uint64MergeOperator"
}

// json merge operator
type JsonMergeOperator struct{}

func (op *JsonMergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var obj interface{}
	err := json.Unmarshal(existingValue, &obj)
	if err != nil {
		return nil, false
	}
	for _, operand := range operands {
		arr := strings.SplitN(string(operand), " ", 3)
		op := arr[0]
		jsonKey := arr[1]
		val := arr[2]

		switch strings.ToLower(op) {
		case "append":
			if jsonKey == "" {
				docObj := obj.([]interface{})
				docObj = append(docObj, val)
			} else {
				docObj := obj.(map[string]interface{})
				arrObj := docObj[jsonKey].([]interface{})
				arrObj = append(arrObj, val)
				docObj[jsonKey] = arrObj
			}
		case "inc":
		default:

		}
	}
	return nil, true
}

func (op *JsonMergeOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return nil, false
}

func (op *JsonMergeOperator) Name() string {
	return "JsonMergeOperator"
}
