package clickhouse

import (
	"database/sql/driver"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConverter(t *testing.T) {
	testCases := []struct {
		value    interface{}
		expected driver.Value
		msg      string
	}{
		{int64(maxAllowedUInt64), int64(9223372036854775807), "int64(maxAllowedUInt64)"},
		{uint64(maxAllowedUInt64 + 1), []byte("9223372036854775808"), "uint64(maxAllowedUInt64+1)"},
		{uint64(maxAllowedUInt64*2 + 1), []byte("18446744073709551615"), "uint64(maxUInt64)"},
	}

	for _, tc := range testCases {
		dv, err := converter{}.ConvertValue(tc.value)
		if assert.NoError(t, err) {
			// assert.ElementsMatch(t, dv, tc.expected, "failed to convert "+tc.msg)
			if !reflect.DeepEqual(tc.expected, dv) {
				t.Errorf("failed to convert %s", tc.msg)
			}
		}
	}
}
