package clickhouse

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestColumnType(t *testing.T) {
	testCases := []struct {
		tt       string
		expected reflect.Type
	}{
		{"Int8", reflect.TypeOf(int8(0))},
		{"Int16", reflect.TypeOf(int16(0))},
		{"Int32", reflect.TypeOf(int32(0))},
		{"Int64", reflect.TypeOf(int64(0))},
		{"UInt8", reflect.TypeOf(uint8(0))},
		{"UInt16", reflect.TypeOf(uint16(0))},
		{"UInt32", reflect.TypeOf(uint32(0))},
		{"UInt64", reflect.TypeOf(uint64(0))},
		{"Float32", reflect.TypeOf(float32(0))},
		{"Float64", reflect.TypeOf(float64(0))},
		{"Date", reflect.TypeOf(time.Time{})},
		{"DateTime", reflect.TypeOf(time.Time{})},
		{"String", reflect.TypeOf("")},
		{"FixedString(5)", reflect.TypeOf("")},
		{"Enum8('one'=1)", reflect.TypeOf("")},
		{"Enum16('one'=1)", reflect.TypeOf("")},
		{"Array(UInt32)", reflect.TypeOf([]uint32{})},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expected, columnType(tc.tt))
	}
}
