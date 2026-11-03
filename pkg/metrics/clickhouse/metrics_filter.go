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

// MetricsFilter decides whether a given ClickHouse metric name should be excluded
// from emission.
//
// Declared in the consumer package (where Exporter and CHIPrometheusWriter live)
// rather than alongside any specific implementation — Go's "accept interfaces,
// return concrete types" idiom. Implementations live as standalone types in
// the `filters` sub-package; new filter strategies (allowlist, glob, etc.) can
// be added there without touching this file.
//
// Nil-safe by contract: implementations must return false from IsExcluded when
// the receiver is nil so callers can pass a typed-nil filter as a no-op. A raw-nil
// interface value is handled by the package-level IsExcluded helper below — see it
// for the shared call-site guard.
type MetricsFilter interface {
	IsExcluded(name string) bool
}

// IsExcluded is a nil-safe wrapper over MetricsFilter: a raw-nil interface value
// is treated as a no-op filter (excludes nothing). Both filter call sites — the
// fetch-side row scan and the writer-side emission — funnel through this so the
// nil-guard contract lives in one place instead of being duplicated per call site.
func IsExcluded(filter MetricsFilter, name string) bool {
	return (filter != nil) && filter.IsExcluded(name)
}
