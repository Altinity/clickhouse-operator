// +build go1.8

package clickhouse

import (
	"context"
	"database/sql/driver"
	"time"
)

var (
	_ driver.StmtExecContext  = new(stmt)
	_ driver.StmtQueryContext = new(stmt)
)

func (s *stmtSuite) TestQueryContext() {
	ctx, cancel := context.WithCancel(context.Background())
	st, err := s.conn.PrepareContext(ctx, "SELECT SLEEP ?")
	s.Require().NoError(err)
	time.AfterFunc(5*time.Millisecond, cancel)
	_, err = st.QueryContext(ctx, 5)
	s.EqualError(err, "context canceled")
	s.NoError(st.Close())
}

func (s *stmtSuite) TestExecContext() {
	ctx, cancel := context.WithCancel(context.Background())
	st, err := s.conn.PrepareContext(ctx, "SELECT SLEEP ?")
	s.Require().NoError(err)
	time.AfterFunc(5*time.Millisecond, cancel)
	_, err = st.ExecContext(ctx, 5)
	s.EqualError(err, "context canceled")
	s.NoError(st.Close())
}

func (s *stmtSuite) TestExecMultiContext() {
	ctx, cancel := context.WithCancel(context.Background())
	tx, err := s.conn.BeginTx(ctx, nil)
	s.Require().NoError(err)
	st, err := tx.PrepareContext(ctx, "SELECT SLEEP ?")
	s.Require().NoError(err)
	time.AfterFunc(10*time.Millisecond, cancel)
	_, err = st.ExecContext(ctx, 5)
	s.EqualError(err, "context canceled")
	s.NoError(st.Close())
}
