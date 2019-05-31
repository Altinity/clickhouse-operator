package clickhouse

import (
	"database/sql"
	"database/sql/driver"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/stretchr/testify/suite"
)

var (
	_ driver.Driver = new(chDriver)
)

var ddls = []string{
	`DROP TABLE IF EXISTS data`,
	`CREATE TABLE data (
			i64 Int64,
			u64 UInt64,
			f64 Float64,
			s   String,
			s2  String,
			a   Array(Int16),
			d   Date,
			t   DateTime,
			e   Enum8('one'=1, 'two'=2, 'three'=3)
	) ENGINE = Memory`,
	`INSERT INTO data VALUES
	 	(-1, 1, 1.0, '1', '1', [1], '2011-03-06', '2011-03-06 06:20:00', 'one'),
	 	(-2, 2, 2.0, '2', '2', [2], '2012-05-31', '2012-05-31 11:20:00', 'two'),
	 	(-3, 3, 3.0, '3', '2', [3], '2016-04-04', '2016-04-04 11:30:00', 'three')
	`,
}

var initialzer = new(dbInit)

type dbInit struct {
	mu   sync.Mutex
	done bool
}

type chSuite struct {
	suite.Suite
	conn *sql.DB
}

func (s *chSuite) SetupSuite() {
	dsn := os.Getenv("TEST_CLICKHOUSE_DSN")
	if len(dsn) == 0 {
		dsn = "http://localhost:8123/default"
	}
	conn, err := sql.Open("clickhouse", dsn)
	s.Require().NoError(err)
	s.Require().NoError(initialzer.Do(conn))
	s.conn = conn
}

func (s *chSuite) TearDownSuite() {
	s.conn.Close()
	_, err := s.conn.Query("SELECT 1")
	s.EqualError(err, "sql: database is closed")
}

func (d *dbInit) Do(conn *sql.DB) error {
	if d.done {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.done {
		return nil
	}
	for _, ddl := range ddls {
		if _, err := conn.Exec(ddl); err != nil {
			return err
		}
	}
	d.done = true
	return nil
}

func scanValues(rows *sql.Rows, template []interface{}) (interface{}, error) {
	var result [][]interface{}
	types := make([]reflect.Type, len(template))
	for i, v := range template {
		types[i] = reflect.TypeOf(v)
	}
	ptrs := make([]interface{}, len(types))
	var err error
	for rows.Next() {
		if err = rows.Err(); err != nil {
			return nil, err
		}
		for i, t := range types {
			ptrs[i] = reflect.New(t).Interface()
		}
		err = rows.Scan(ptrs...)
		if err != nil {
			return nil, err
		}
		values := make([]interface{}, len(types))
		for i, p := range ptrs {
			values[i] = reflect.ValueOf(p).Elem().Interface()
		}
		result = append(result, values)
	}
	return result, nil
}

func parseTime(layout, s string) time.Time {
	t, err := time.Parse(layout, s)
	if err != nil {
		panic(err)
	}
	return t
}

func parseDate(s string) time.Time {
	return parseTime(dateFormat, s)
}

func parseDateTime(s string) time.Time {
	return parseTime(timeFormat, s)
}
