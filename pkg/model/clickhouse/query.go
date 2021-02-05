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

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
)

// Query
type Query struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	Rows       *databasesql.Rows
}

// NewQuery
func NewQuery(ctx context.Context, cancelFunc context.CancelFunc, rows *databasesql.Rows) *Query {
	return &Query{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		Rows:       rows,
	}
}

// Close
func (q *Query) Close() {
	if q == nil {
		return
	}

	if q.Rows != nil {
		err := q.Rows.Close()
		q.Rows = nil
		if err != nil {
			log.A().Error("UNABLE to close rows. Err: %v", err)
		}
	}

	if q.cancelFunc != nil {
		q.cancelFunc()
		q.cancelFunc = nil
	}
}
