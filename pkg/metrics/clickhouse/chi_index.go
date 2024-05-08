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

import "github.com/altinity/clickhouse-operator/pkg/apis/metrics"

type chInstallationsIndex map[string]*metrics.WatchedCHI

func (i chInstallationsIndex) slice() []*metrics.WatchedCHI {
	res := make([]*metrics.WatchedCHI, 0)
	for _, chi := range i {
		res = append(res, chi)
	}
	return res
}

func (i chInstallationsIndex) get(key string) (*metrics.WatchedCHI, bool) {
	if i == nil {
		return nil, false
	}
	if _, ok := i[key]; ok {
		return i[key], true
	}
	return nil, false
}

func (i chInstallationsIndex) set(key string, value *metrics.WatchedCHI) {
	if i == nil {
		return
	}
	i[key] = value
}

func (i chInstallationsIndex) remove(key string) {
	if i == nil {
		return
	}
	if _, ok := i[key]; ok {
		delete(i, key)
	}
}

func (i chInstallationsIndex) walk(f func(*metrics.WatchedCHI, *metrics.WatchedCluster, *metrics.WatchedHost)) {
	// Loop over ClickHouseInstallations
	for _, chi := range i {
		chi.WalkHosts(f)
	}
}
