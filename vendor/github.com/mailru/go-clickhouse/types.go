package clickhouse

import (
	"database/sql/driver"
	"fmt"
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

// Decimal32 converts value to Decimal32 of precision S.
// The value can be a number or a string. The S (scale) parameter specifies the number of decimal places.
func Decimal32(v interface{}, s int32) driver.Valuer {
	return decimal{32, s, v}
}

// Decimal64 converts value to Decimal64 of precision S.
// The value can be a number or a string. The S (scale) parameter specifies the number of decimal places.
func Decimal64(v interface{}, s int32) driver.Valuer {
	return decimal{64, s, v}
}

// Decimal128 converts value to Decimal128 of precision S.
// The value can be a number or a string. The S (scale) parameter specifies the number of decimal places.
func Decimal128(v interface{}, s int32) driver.Valuer {
	return decimal{128, s, v}
}

type array struct {
	v interface{}
}

// Value implements driver.Valuer
func (a array) Value() (driver.Value, error) {
	return textEncode.Encode(a)
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

type decimal struct {
	p int32
	s int32
	v interface{}
}

// Value implements driver.Valuer
func (d decimal) Value() (driver.Value, error) {
	return []byte(fmt.Sprintf("toDecimal%d(%v, %d)", d.p, d.v, d.s)), nil
}
