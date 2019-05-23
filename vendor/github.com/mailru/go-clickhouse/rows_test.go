package clickhouse

import (
	"database/sql/driver"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTextRows(t *testing.T) {
	rows, err := newTextRows([]byte("Number\tText\nInt32\tString\n1\t'hello'\n2\t'world'\n"), time.Local, false)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []string{"Number", "Text"}, rows.Columns())
	assert.Equal(t, []string{"Int32", "String"}, rows.types)
	assert.Equal(t, reflect.TypeOf(int32(0)), rows.ColumnTypeScanType(0))
	assert.Equal(t, reflect.TypeOf(""), rows.ColumnTypeScanType(1))
	assert.Equal(t, "Int32", rows.ColumnTypeDatabaseTypeName(0))
	assert.Equal(t, "String", rows.ColumnTypeDatabaseTypeName(1))

	assert.Equal(t, time.Local, rows.decode.(*textDecoder).location)
	dest := make([]driver.Value, 2)
	if !assert.NoError(t, rows.Next(dest)) {
		return
	}
	assert.Equal(t, []driver.Value{int32(1), "hello"}, dest)
	if !assert.NoError(t, rows.Next(dest)) {
		return
	}
	assert.Equal(t, []driver.Value{int32(2), "world"}, dest)
	assert.Equal(t, 0, len(rows.data))
	assert.Equal(t, io.EOF, rows.Next(dest))
	assert.NoError(t, rows.Close())
	assert.Nil(t, rows.data)
}
