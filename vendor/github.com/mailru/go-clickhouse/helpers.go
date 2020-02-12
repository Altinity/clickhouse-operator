package clickhouse

import (
	"bytes"
	"net/http"
	"strings"
	"time"
)

var (
	escaper    = strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	dateFormat = "2006-01-02"
	timeFormat = "2006-01-02 15:04:05"
)

func escape(s string) string {
	return escaper.Replace(s)
}

func quote(s string) string {
	return "'" + s + "'"
}

func formatTime(value time.Time) string {
	return quote(value.Format(timeFormat))
}

func formatDate(value time.Time) string {
	return quote(value.Format(dateFormat))
}

func readResponse(response *http.Response) (result []byte, err error) {
	if response.ContentLength > 0 {
		result = make([]byte, 0, response.ContentLength)
	}
	buf := bytes.NewBuffer(result)
	defer response.Body.Close()
	_, err = buf.ReadFrom(response.Body)
	result = buf.Bytes()
	return
}

func numOfColumns(data []byte) int {
	var cnt int
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
	var i, k int
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
