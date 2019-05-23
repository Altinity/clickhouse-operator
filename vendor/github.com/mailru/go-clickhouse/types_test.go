package clickhouse

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestArray(t *testing.T) {
	testCases := []struct {
		value    interface{}
		expected driver.Value
	}{
		{[]int16{1, 2}, []byte("[1,2]")},
		{[]int32{1, 2}, []byte("[1,2]")},
		{[]int64{1, 2}, []byte("[1,2]")},
		{[]uint16{1, 2}, []byte("[1,2]")},
		{[]uint32{1, 2}, []byte("[1,2]")},
		{[]uint64{1, 2}, []byte("[1,2]")},
		{[]uint64{}, []byte("[]")},
	}

	for _, tc := range testCases {
		dv, err := Array(tc.value).Value()
		if assert.NoError(t, err) {
			assert.Equal(t, tc.expected, dv)
		}
	}
}

func TestDate(t *testing.T) {
	d := time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local)
	dv, err := Date(d).Value()
	if assert.NoError(t, err) {
		assert.Equal(t, []byte("'2016-04-04'"), dv)
	}
}

func TestUInt64(t *testing.T) {
	u := uint64(1) << 63
	dv, err := UInt64(u).Value()
	if assert.NoError(t, err) {
		assert.Equal(t, []byte("9223372036854775808"), dv)
	}
}
