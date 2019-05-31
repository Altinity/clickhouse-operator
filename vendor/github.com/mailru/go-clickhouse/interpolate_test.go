package clickhouse

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInterpolate(t *testing.T) {
	testCases := []struct {
		query    string
		args     []driver.Value
		expected string
	}{
		{"?", []driver.Value{1}, "1"},
		{"SELECT ?", []driver.Value{1}, "SELECT 1"},
		{"SELECT ?, ?", []driver.Value{1, 2.0}, "SELECT 1, 2"},
		{"SELECT a='?' AND b=?", []driver.Value{"1"}, "SELECT a='?' AND b='1'"},
		{"SELECT a=? AND b='?'", []driver.Value{"1"}, "SELECT a='1' AND b='?'"},
		{"SELECT '\\'', ?", []driver.Value{"1"}, "SELECT '\\'', '1'"},
	}

	for _, tc := range testCases {
		v, err := interpolateParams(tc.query, tc.args)
		if assert.NoError(t, err) {
			assert.Equal(t, tc.expected, v)
		}
	}
	_, err := interpolateParams("SELECT ?, ?", []driver.Value{1})
	assert.Equal(t, ErrPlaceholderCount, err)
}
