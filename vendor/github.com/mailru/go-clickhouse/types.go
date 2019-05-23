package clickhouse

import (
	"database/sql/driver"
	"strconv"
	"time"
)

// Array wraps slice or array into driver.Valuer interface to allow pass through it from database/sql
func Array(v interface{}) driver.Valuer {
	return array{v: v}
}

// Date returns date for t
func Date(t time.Time) driver.Valuer {
	return date(t)
}

// UInt64 returns date for t
func UInt64(u uint64) driver.Valuer {
	return bigUint64(u)
}

type array struct {
	v interface{}
}

// Value implements driver.Valuer
func (a array) Value() (driver.Value, error) {
	return []byte(textEncode.Encode(a.v)), nil
}

type date time.Time

// Value implements driver.Valuer
func (d date) Value() (driver.Value, error) {
	return []byte(formatDate(time.Time(d))), nil
}

type bigUint64 uint64

// Value implements driver.Valuer
func (u bigUint64) Value() (driver.Value, error) {
	return []byte(strconv.FormatUint(uint64(u), 10)), nil
}
