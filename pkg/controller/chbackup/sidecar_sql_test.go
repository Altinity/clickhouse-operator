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

package chbackup

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A sidecar is busy when clickhouse-backup tells that another operation runs, and only then: the digits of a backup
// name or of a date the error echoes are no status code.
func TestBusyError(t *testing.T) {
	require.True(t, busyError(errors.New("code: 86, message: Received error from remote server /backup/actions. "+
		"HTTP status code: 500 Internal Server Error, body: {\"status\":\"error\",\"error\":\"another operation is currently running\"}")))
	require.False(t, busyError(errors.New("code: 86, message: HTTP status code: 500, body: create_remote shard1-20260423010000: no such table")))
	require.False(t, busyError(errors.New("code: 86, message: HTTP status code: 500, body: restore b409: backup b409 not found")))
	require.False(t, busyError(errors.New("connection refused")))
}

// The insert of a command sets the HTTP timeouts of the URL engine towards the sidecar, so a synchronous delete that
// takes long is not cut by the default of ClickHouse.
func TestPushQuerySetsTheHTTPTimeouts(t *testing.T) {
	require.Equal(t, "INSERT INTO system.backup_actions (command) SELECT 'delete remote b1' SETTINGS async_insert = 0, "+
		"http_receive_timeout = 14400, http_send_timeout = 14400", pushQuery("delete remote b1", 4*time.Hour))
	require.Contains(t, pushQuery("create_remote --rbac b'1", 0), "SELECT 'create_remote --rbac b\\'1' SETTINGS async_insert = 0, "+
		"http_receive_timeout = 1, http_send_timeout = 1")
}
