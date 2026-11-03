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

package filters

import (
	"testing"
)

func TestRegexp(t *testing.T) {
	f := NewRegexp([]string{
		`^metric\.OS.*CPU[0-9]+$`,
		`^table_parts$`,
		`[`,
		``,
	})

	if !f.IsExcluded("metric.OSUserTimeCPU12") {
		t.Fatal("expected CPU metric to be excluded")
	}
	if !f.IsExcluded("table_parts") {
		t.Fatal("expected table_parts metric to be excluded")
	}
	if f.IsExcluded("metric.OSUserTime") {
		t.Fatal("expected non-CPU metric to be included")
	}
	if len(f.regexps) != 2 {
		t.Fatalf("expected 2 compiled regexps (invalid `[` and empty `` skipped), got %d", len(f.regexps))
	}
}

func TestRegexpNilSafe(t *testing.T) {
	var f *Regexp
	if f.IsExcluded("anything") {
		t.Fatal("nil filter should not exclude")
	}
}
