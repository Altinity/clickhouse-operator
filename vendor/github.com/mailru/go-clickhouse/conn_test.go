package clickhouse

import (
	"database/sql"
	"database/sql/driver"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

var (
	_ driver.Conn    = new(conn)
	_ driver.Execer  = new(conn)
	_ driver.Queryer = new(conn)
	_ driver.Tx      = new(conn)
)

type connSuite struct {
	chSuite
}

func (s *connSuite) TestQuery() {
	testCases := []struct {
		query    string
		args     []interface{}
		expected [][]interface{}
	}{
		{"SELECT i64 AS num FROM data WHERE i64<0", nil, [][]interface{}{{int64(-1)}, {int64(-2)}, {int64(-3)}}},
		{"SELECT i64 AS num FROM data WHERE i64<?", []interface{}{-3}, [][]interface{}{}},
		{"SELECT i64 AS num FROM data WHERE i64=?", []interface{}{nil}, [][]interface{}{}},
		{"SELECT i64 AS num FROM data WHERE i64=?", []interface{}{-1}, [][]interface{}{{int64(-1)}}},
		{
			"SELECT * FROM data WHERE u64=?",
			[]interface{}{1},
			[][]interface{}{{int64(-1), uint64(1), float64(1), "1", "1", []int16{1},
				parseDate("2011-03-06"), parseDateTime("2011-03-06 06:20:00"), "one"}},
		},
		{
			"SELECT i64, count() FROM data WHERE i64<0 GROUP BY i64 WITH TOTALS ORDER BY i64",
			nil,
			[][]interface{}{{int64(-3), int64(1)}, {int64(-2), int64(1)}, {int64(-1), int64(1)}, {int64(0), int64(3)}},
		},
	}

	for _, tc := range testCases {
		rows, err := s.conn.Query(tc.query, tc.args...)
		if !s.NoError(err) {
			continue
		}
		if len(tc.expected) == 0 {
			s.False(rows.Next())
			s.NoError(rows.Err())
		} else {
			v, err := scanValues(rows, tc.expected[0])
			if s.NoError(err) {
				s.Equal(tc.expected, v)
			}
		}
		s.NoError(rows.Close())
	}
}

func (s *connSuite) TestExec() {
	testCases := []struct {
		query  string
		query2 string
		args   []interface{}
	}{
		{
			"INSERT INTO data (i64) VALUES (?)",
			"SELECT i64 FROM data WHERE i64=?",
			[]interface{}{int64(1)},
		},
		{
			"INSERT INTO data (i64, u64) VALUES (?, ?)",
			"SELECT i64, u64 FROM data WHERE i64=? AND u64=?",
			[]interface{}{int64(2), uint64(12)},
		},
		{
			"INSERT INTO data (i64, a) VALUES (?, ?)",
			"",
			[]interface{}{int64(3), Array([]int16{1, 2})},
		},
		{
			"INSERT INTO data (u64) VALUES (?)",
			"",
			[]interface{}{UInt64(uint64(1) << 63)},
		},
		{
			"INSERT INTO data (d, t) VALUES (?, ?)",
			"",
			[]interface{}{
				Date(time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local)),
				time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local),
			},
		},
	}
	for _, tc := range testCases {
		result, err := s.conn.Exec(tc.query, tc.args...)
		if !s.NoError(err) {
			continue
		}
		s.NotNil(result)
		_, err = result.LastInsertId()
		s.Equal(ErrNoLastInsertID, err)
		_, err = result.RowsAffected()
		s.Equal(ErrNoRowsAffected, err)
		if len(tc.query2) == 0 {
			continue
		}
		rows, err := s.conn.Query(tc.query2, tc.args...)
		if !s.NoError(err) {
			continue
		}
		v, err := scanValues(rows, tc.args)
		if s.NoError(err) {
			s.Equal([][]interface{}{tc.args}, v)
		}
		s.NoError(rows.Close())
	}
}

func (s *connSuite) TestCommit() {
	tx, err := s.conn.Begin()
	s.Require().NoError(err)
	_, err = tx.Exec("INSERT INTO data (i64) VALUES (5)")
	s.Require().NoError(err)
	s.NoError(tx.Commit())
}

func (s *connSuite) TestRollback() {
	tx, err := s.conn.Begin()
	s.Require().NoError(err)
	_, err = tx.Exec("INSERT INTO data (i64) VALUES (6)")
	s.Require().NoError(err)
	s.Equal(sql.ErrTxDone, tx.Rollback())
}

func (s *connSuite) TestServerError() {
	_, err := s.conn.Query("SELECT 1 FROM '???'")
	srvErr, ok := err.(*Error)
	s.Require().True(ok)
	s.Equal(62, srvErr.Code)
	s.Contains(srvErr.Message, "Syntax error:")
	s.Contains(srvErr.Error(), "Code: 62, Message: Syntax error:")
}

func (s *connSuite) TestBuildRequestReadonlyWithAuth() {
	cfg := NewConfig()
	cfg.User = "user"
	cfg.Password = "password"
	cn := newConn(cfg)
	req, err := cn.buildRequest("SELECT 1", nil, true)
	if s.NoError(err) {
		user, password, ok := req.BasicAuth()
		s.True(ok)
		s.Equal("user", user)
		s.Equal("password", password)
		s.Equal(http.MethodGet, req.Method)
		s.Equal(cn.url.String(), req.URL.String())
		s.Nil(req.URL.User)
	}
}

func (s *connSuite) TestBuildRequestReadWriteWOAuth() {
	cn := newConn(NewConfig())
	req, err := cn.buildRequest("INSERT 1 INTO num", nil, false)
	if s.NoError(err) {
		_, _, ok := req.BasicAuth()
		s.False(ok)
		s.Equal(http.MethodPost, req.Method)
		s.Equal(cn.url.String(), req.URL.String())
	}
}

func TestConn(t *testing.T) {
	suite.Run(t, new(connSuite))
}
