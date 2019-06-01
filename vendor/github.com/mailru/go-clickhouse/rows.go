package clickhouse

import (
	"database/sql/driver"
	"io"
	"reflect"
	"time"
)

type textRows struct {
	columns []string
	types   []string
	data    []byte
	decode  decoder
}

// Columns returns the columns names
func (r *textRows) Columns() []string {
	return r.columns
}

// ColumnTypeScanType implements the driver.RowsColumnTypeScanType
func (r *textRows) ColumnTypeScanType(index int) reflect.Type {
	return columnType(r.types[index])
}

// ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypeDatabaseTypeName
func (r *textRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.types[index]
}

// Close closes the rows iterator.
func (r *textRows) Close() error {
	r.data = nil
	return nil
}

// Next is called to populate the next row of data into
func (r *textRows) Next(dest []driver.Value) error {
	i, k := 0, 0
	var err error
	for j, ch := range r.data {
		switch ch {
		case '\t':
			dest[k], err = r.decode.Decode(r.types[k], r.data[i:j])
			if err != nil {
				return err
			}
			k++
			i = j + 1
		case '\n':
			if j == 0 {
				// totals are separated by empty line
				i = j + 1
				continue
			}
			dest[k], err = r.decode.Decode(r.types[k], r.data[i:j])
			r.data = r.data[j+1:]
			return err
		}
	}
	return io.EOF
}

func newTextRows(data []byte, location *time.Location, useDBLocation bool) (*textRows, error) {
	colCount := numOfColumns(data)
	if colCount < 0 {
		return nil, ErrMalformed
	}
	columns := make([]string, colCount)
	types := make([]string, colCount)
	data = data[splitTSV(data, columns):]
	data = data[splitTSV(data, types):]
	return &textRows{columns: columns, types: types, data: data, decode: &textDecoder{location: location, useDBLocation: useDBLocation}}, nil
}
