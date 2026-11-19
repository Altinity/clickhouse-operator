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
	"time"
)

// A host without the key_values column is probed again after this long, so an in place
// upgrade to ClickHouse 26.8 is noticed without restarting the exporter.
const asyncKeyValuesNegativeTTL = 5 * time.Minute

type asyncKeyValuesEntry struct {
	supported bool
	probedAt  time.Time
}

// asyncKeyValuesCache remembers per host whether system.asynchronous_metrics has the key_values column.
// A positive answer is kept until a metrics query fails (a downgrade), a negative one expires.
type asyncKeyValuesCache struct {
	mu    sync.Mutex
	hosts map[string]asyncKeyValuesEntry
}

func newAsyncKeyValuesCache() *asyncKeyValuesCache {
	return &asyncKeyValuesCache{hosts: map[string]asyncKeyValuesEntry{}}
}

func (c *asyncKeyValuesCache) get(host string) (supported, known bool) {
	if c == nil {
		return false, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.hosts[host]
	if !ok || (!entry.supported && time.Since(entry.probedAt) > asyncKeyValuesNegativeTTL) {
		return false, false
	}
	return entry.supported, true
}

func (c *asyncKeyValuesCache) set(host string, supported bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hosts[host] = asyncKeyValuesEntry{supported: supported, probedAt: time.Now()}
}

func (c *asyncKeyValuesCache) forget(host string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.hosts, host)
}
