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

// Package filters provides MetricsFilter implementations for the ClickHouse
// metrics exporter. The MetricsFilter interface itself is declared in the
// consumer package (pkg/metrics/clickhouse) — Go's "accept interfaces,
// return concrete types" idiom — and implementations satisfy it structurally.
//
// Add new filter strategies (allowlist, glob, label-based, etc.) as siblings
// of Regexp in this package.
package filters

import (
	"regexp"

	log "github.com/golang/glog"
)

// Regexp is a MetricsFilter implementation that excludes metric names matching
// any of a list of compiled regexps (OR-of-matches semantics).
type Regexp struct {
	regexps []*regexp.Regexp
}

// NewRegexp builds a Regexp filter from the given pattern list. Empty patterns
// are skipped; patterns that fail to compile are logged at warning level and
// skipped (the filter degrades to fewer exclusions rather than failing the
// process — the trade-off is appropriate for a metrics exporter where
// availability of partial telemetry beats a hard fail on a typo'd regexp).
func NewRegexp(patterns []string) *Regexp {
	filter := &Regexp{}
	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			log.Warningf("Ignore invalid ClickHouse metric exclusion regexp %q: %v", pattern, err)
			continue
		}
		filter.regexps = append(filter.regexps, re)
	}
	return filter
}

// IsExcluded reports whether name matches any compiled exclusion regexp.
// Nil-receiver safe: returns false when the filter is unset.
func (f *Regexp) IsExcluded(name string) bool {
	if f == nil {
		return false
	}
	for _, re := range f.regexps {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}
