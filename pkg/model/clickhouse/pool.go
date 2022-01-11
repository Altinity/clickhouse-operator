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
	"sync"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
)

var (
	dbConnectionPool               = sync.Map{}
	dbConnectionPoolEntryInitMutex = sync.Mutex{}
)

// GetPooledDBConnection gets connection out of the pool.
// In case no connection available new connection is created and returned.
func GetPooledDBConnection(params *ConnectionParams) *Connection {
	key := makePoolKey(params)

	if connection, existed := dbConnectionPool.Load(key); existed {
		log.V(2).F().Info("Found pooled connection: %s", params.GetDSNWithHiddenCredentials())
		return connection.(*Connection)
	}

	// Pooled connection not found, need to add it to the pool

	dbConnectionPoolEntryInitMutex.Lock()
	defer dbConnectionPoolEntryInitMutex.Unlock()

	// Double check for race condition
	if connection, existed := dbConnectionPool.Load(key); existed {
		log.V(2).F().Info("Found pooled connection: %s", params.GetDSNWithHiddenCredentials())
		return connection.(*Connection)
	}

	log.V(2).F().Info("Add connection to the pool: %s", params.GetDSNWithHiddenCredentials())
	dbConnectionPool.Store(key, NewConnection(params))

	// Fetch from the pool
	if connection, existed := dbConnectionPool.Load(key); existed {
		log.V(2).F().Info("Found pooled connection: %s", params.GetDSNWithHiddenCredentials())
		return connection.(*Connection)
	}

	return nil
}

// DropHost deletes host from the pool
// TODO we need to be able to remove entries from the pool
func DropHost(host string) {

}

// makePoolKey makes key out of connection params to be used by the pool
func makePoolKey(params *ConnectionParams) string {
	return params.GetDSN()
}
