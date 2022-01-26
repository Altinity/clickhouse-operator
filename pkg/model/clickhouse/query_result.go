// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clickhouse

import (
	"context"
	databasesql "database/sql"
	"fmt"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
)

// QueryResult specifies result of a query
type QueryResult struct {
	// Query execution context
	ctx        context.Context
	cancelFunc context.CancelFunc
	// Query result rows
	Rows *databasesql.Rows
}

// NewQueryResult creates new query result
func NewQueryResult(ctx context.Context, cancelFunc context.CancelFunc, rows *databasesql.Rows) *QueryResult {
	return &QueryResult{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		Rows:       rows,
	}
}

// Close closes query result and releases all allocated resources.
// Should be called on each query result
func (q *QueryResult) Close() {
	if q == nil {
		return
	}

	if q.Rows != nil {
		err := q.Rows.Close()
		q.Rows = nil
		if err != nil {
			log.F().Error("UNABLE to close rows. Err: %v", err)
		}
	}

	if q.cancelFunc != nil {
		q.cancelFunc()
		q.cancelFunc = nil
	}
}

// UnzipColumnsAsStrings splits result table into string columns
func (q *QueryResult) UnzipColumnsAsStrings(columns ...*[]string) error {
	if q == nil {
		return fmt.Errorf("empty query")
	}
	if q.Rows == nil {
		return fmt.Errorf("no rows")
	}

	// Results
	row := make([]string, len(columns), len(columns))
	pointers := make([]interface{}, len(columns), len(columns))
	for i := range row {
		pointers[i] = &row[i]
	}

	// Scan rows
	for q.Rows.Next() {
		if err := q.Rows.Scan(pointers...); err != nil {
			log.V(1).F().Error("UNABLE to scan row err: %v", err)
			return err
		}
		for i := range columns {
			*columns[i] = append(*columns[i], row[i])
		}
	}
	return nil
}

// Int fetches one int from the query result
func (q *QueryResult) Int() (int, error) {
	if q == nil {
		return 0, fmt.Errorf("empty query")
	}
	if q.Rows == nil {
		return 0, fmt.Errorf("no rows")
	}

	var result int
	for q.Rows.Next() {
		if err := q.Rows.Scan(&result); err != nil {
			log.V(1).F().Error("UNABLE to scan row err: %v", err)
			return 0, err
		}
		return result, nil
	}
	return 0, fmt.Errorf("found no rows")
}
