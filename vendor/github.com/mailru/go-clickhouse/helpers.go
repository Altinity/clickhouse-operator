package clickhouse

import (
	"bytes"
	"net/http"
	"reflect"
	"strings"
	"time"
)

var (
	escaper   = strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	unescaper = strings.NewReplacer(`\\`, `\`, `\'`, `'`)
)

func escape(s string) string {
	return escaper.Replace(s)
}

func unescape(s string) string {
	return unescaper.Replace(s)
}

func quote(s string) string {
	return "'" + s + "'"
}

func unquote(s string) string {
	if len(s) > 0 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}

func formatTime(value time.Time) string {
	return quote(value.Format(timeFormat))
}

func formatDate(value time.Time) string {
	return quote(value.Format(dateFormat))
}

func readResponse(response *http.Response) (result []byte, err error) {
	if response.ContentLength > 0 {
		result = make([]byte, response.ContentLength)
	}
	buf := bytes.NewBuffer(result)
	defer response.Body.Close()
	_, err = buf.ReadFrom(response.Body)
	result = buf.Bytes()
	return
}

func numOfColumns(data []byte) int {
	cnt := 0
	for _, ch := range data {
		switch ch {
		case '\t':
			cnt++
		case '\n':
			return cnt + 1
		}
	}
	return -1
}

// splitTSV splits one row of tab separated values, returns begin of next row
func splitTSV(data []byte, out []string) int {
	i := 0
	k := 0
	for j, ch := range data {
		switch ch {
		case '\t':
			out[k] = string(data[i:j])
			k++
			i = j + 1
		case '\n':
			out[k] = string(data[i:j])
			return j + 1
		}
	}
	return -1
}

func columnType(name string) reflect.Type {
	switch name {
	case "Date", "DateTime":
		return reflect.ValueOf(time.Time{}).Type()
	case "UInt8":
		return reflect.ValueOf(uint8(0)).Type()
	case "UInt16":
		return reflect.ValueOf(uint16(0)).Type()
	case "UInt32":
		return reflect.ValueOf(uint32(0)).Type()
	case "UInt64":
		return reflect.ValueOf(uint64(0)).Type()
	case "Int8":
		return reflect.ValueOf(int8(0)).Type()
	case "Int16":
		return reflect.ValueOf(int16(0)).Type()
	case "Int32":
		return reflect.ValueOf(int32(0)).Type()
	case "Int64":
		return reflect.ValueOf(int64(0)).Type()
	case "Float32":
		return reflect.ValueOf(float32(0)).Type()
	case "Float64":
		return reflect.ValueOf(float64(0)).Type()
	case "String":
		return reflect.ValueOf("").Type()
	}
	if strings.HasPrefix(name, "FixedString") {
		return reflect.ValueOf("").Type()
	}
	if strings.HasPrefix(name, "Array") {
		subType := columnType(name[6 : len(name)-1])
		if subType != nil {
			return reflect.SliceOf(subType)
		}
		return nil
	}
	if strings.HasPrefix(name, "Enum") {
		return reflect.ValueOf("").Type()
	}
	return nil
}
