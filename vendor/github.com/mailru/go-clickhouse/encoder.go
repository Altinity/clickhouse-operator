package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

var (
	textEncode encoder = new(textEncoder)
)

type encoder interface {
	Encode(value driver.Value) ([]byte, error)
}

type textEncoder struct {
}

// Encode encodes driver value into string
// Note: there is 2 convention:
// type string will be quoted
// type []byte will be encoded as is (raw string)
func (e *textEncoder) Encode(value driver.Value) ([]byte, error) {
	switch v := value.(type) {
	case array:
		return e.encodeArray(reflect.ValueOf(v.v))
	case []byte:
		return v, nil
	}

	vv := reflect.ValueOf(value)
	switch vv.Kind() {
	case reflect.Interface, reflect.Ptr:
		if vv.IsNil() {
			return []byte("NULL"), nil
		}
		return e.Encode(vv.Elem().Interface())
	case reflect.Slice, reflect.Array:
		return e.encodeArray(vv)
	}
	return []byte(e.encode(value)), nil
}

func (e *textEncoder) encode(value driver.Value) string {
	if value == nil {
		return "NULL"
	}
	switch v := value.(type) {
	case bool:
		if v {
			return "1"
		}
		return "0"
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		return quote(escape(v))
	case time.Time:
		return formatTime(v)
	}

	return fmt.Sprint(value)
}

// EncodeArray encodes a go slice or array as Clickhouse Array
func (e *textEncoder) encodeArray(value reflect.Value) ([]byte, error) {
	if value.Kind() != reflect.Slice && value.Kind() != reflect.Array {
		return nil, fmt.Errorf("expected array or slice, got %s", value.Kind())
	}

	res := make([]byte, 0)
	res = append(res, '[')
	for i := 0; i < value.Len(); i++ {
		if i > 0 {
			res = append(res, ',')
		}
		tmp, err := e.Encode(value.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		res = append(res, tmp...)
	}
	return append(res, ']'), nil
}
