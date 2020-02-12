package clickhouse

import (
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

func newTextRows(c *conn, body io.ReadCloser, location *time.Location, useDBLocation bool) (*textRows, error) {
	tsvReader := csv.NewReader(body)
	tsvReader.Comma = '\t'
	tsvReader.LazyQuotes = true

	columns, err := tsvReader.Read()
	if err != nil {
		return nil, err
	}

	types, err := tsvReader.Read()
	if err != nil {
		return nil, err
	}
	for i := range types {
		types[i], err = readUnquoted(strings.NewReader(types[i]), 0)
		if err != nil {
			return nil, err
		}
	}

	parsers := make([]DataParser, len(types), len(types))
	for i, typ := range types {
		desc, err := ParseTypeDesc(typ)
		if err != nil {
			return nil, err
		}

		parsers[i], err = NewDataParser(desc, &DataParserOptions{
			Location:      location,
			UseDBLocation: useDBLocation,
		})
		if err != nil {
			return nil, err
		}
	}

	return &textRows{
		c:        c,
		respBody: body,
		tsv:      tsvReader,
		columns:  columns,
		types:    types,
		parsers:  parsers,
	}, nil
}

type textRows struct {
	c        *conn
	respBody io.ReadCloser
	tsv      *csv.Reader
	columns  []string
	types    []string
	parsers  []DataParser
}

func (r *textRows) Columns() []string {
	return r.columns
}

func (r *textRows) Close() error {
	r.c.cancel = nil
	return r.respBody.Close()
}

func (r *textRows) Next(dest []driver.Value) error {
	row, err := r.tsv.Read()
	if err != nil {
		return err
	}

	for i, s := range row {
		reader := strings.NewReader(s)
		v, err := r.parsers[i].Parse(reader)
		if err != nil {
			return err
		}
		if _, _, err := reader.ReadRune(); err != io.EOF {
			return fmt.Errorf("trailing data after parsing the value")
		}
		dest[i] = v
	}

	return nil
}

// ColumnTypeScanType implements the driver.RowsColumnTypeScanType
func (r *textRows) ColumnTypeScanType(index int) reflect.Type {
	return r.parsers[index].Type()
}

// ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypeDatabaseTypeName
func (r *textRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.types[index]
}
