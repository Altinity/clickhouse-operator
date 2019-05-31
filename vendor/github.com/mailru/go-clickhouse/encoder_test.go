package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTextEncoder(t *testing.T) {
	dt := time.Date(2011, 3, 6, 6, 20, 0, 0, time.UTC)
	d := time.Date(2012, 5, 31, 0, 0, 0, 0, time.UTC)
	testCases := []struct {
		value    interface{}
		expected string
	}{
		{true, "1"},
		{int8(1), "1"},
		{int16(1), "1"},
		{int32(1), "1"},
		{int64(1), "1"},
		{int(-1), "-1"},
		{uint8(1), "1"},
		{uint16(1), "1"},
		{uint32(1), "1"},
		{uint64(1), "1"},
		{uint(1), "1"},
		{float32(1), "1"},
		{float64(1), "1"},
		{dt, "'2011-03-06 06:20:00'"},
		{d, "'2012-05-31 00:00:00'"},
		{"hello", "'hello'"},
		{[]byte("hello"), "hello"},
		{`\\'hello`, `'\\\\\'hello'`},
		{[]byte(`\\'hello`), `\\'hello`},
		{[]int32{1, 2}, "[1,2]"},
		{[]int32{}, "[]"},
		{&d, "'2012-05-31 00:00:00'"},
	}

	enc := new(textEncoder)
	for _, tc := range testCases {
		assert.Equal(t, tc.expected, enc.Encode(tc.value))
	}
}

func TestTextDecoder(t *testing.T) {
	dt := time.Date(2011, 3, 6, 6, 20, 0, 0, time.UTC)
	d := time.Date(2012, 5, 31, 0, 0, 0, 0, time.UTC)
	zerodt := time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)
	testCases := []struct {
		tt       string
		value    string
		expected interface{}
	}{
		{"Int8", "1", int8(1)},
		{"Int16", "1", int16(1)},
		{"Int32", "1", int32(1)},
		{"Int64", "1", int64(1)},
		{"UInt8", "1", uint8(1)},
		{"UInt16", "1", uint16(1)},
		{"UInt32", "1", uint32(1)},
		{"UInt64", "1", uint64(1)},
		{"Float32", "1", float32(1)},
		{"Float64", "1", float64(1)},
		{"Date", "'2012-05-31'", d},
		{"Date", "'0000-00-00'", zerodt},
		{"DateTime", "'2011-03-06 06:20:00'", dt},
		{"DateTime", "'0000-00-00 00:00:00'", zerodt},
		{"DateTime(\\'Europe/Moscow\\')", "'2011-03-06 06:20:00'", dt},
		{"String", "'hello'", "hello"},
		{"String", `'\\\\\'hello'`, `\\'hello`},
		{"FixedString(5)", "'hello'", "hello"},
		{"FixedString(7)", `'\\\\\'hello'`, `\\'hello`},
		{"Enum8('one'=1)", "'one'", "one"},
		{"Enum16('one'=1)", "'one'", "one"},
		{"Array(UInt32)", "[1,2]", []uint32{1, 2}},
		{"Array(UInt32)", "[]", []uint32{}},
	}

	dec := &textDecoder{location: time.UTC, useDBLocation: false}
	for i, tc := range testCases {
		v, err := dec.Decode(tc.tt, []byte(tc.value))
		if assert.NoError(t, err, "%d", i) {
			assert.Equal(t, tc.expected, v)
		}
	}
}

func TestDecodeTimeWithLocation(t *testing.T) {
	dt := time.Date(2011, 3, 6, 3, 20, 0, 0, time.UTC)
	dataType := "DateTime(\\'Europe/Moscow\\')"
	dtStr := "'2011-03-06 06:20:00'"
	dec := &textDecoder{location: time.UTC, useDBLocation: true}

	v, err := dec.Decode(dataType, []byte(dtStr))
	if assert.NoError(t, err) {
		assert.Equal(t, dt, v)
	}
}
