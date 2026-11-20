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

package creator

import (
	"testing"

	apps "k8s.io/api/apps/v1"
)

func TestHostServiceSelector(t *testing.T) {
	hostScope := map[string]string{
		"clickhouse.altinity.com/shard":   "0",
		"clickhouse.altinity.com/replica": "1",
	}
	got := HostServiceSelector(hostScope, "chi-test-default-0-3-0")

	if got[apps.StatefulSetPodNameLabel] != "chi-test-default-0-3-0" {
		t.Fatalf("pod name label = %q, want %q", got[apps.StatefulSetPodNameLabel], "chi-test-default-0-3-0")
	}
	for k, v := range hostScope {
		if got[k] != v {
			t.Fatalf("label %s = %q, want %q", k, got[k], v)
		}
	}
	if len(got) != len(hostScope)+1 {
		t.Fatalf("selector has %d labels, want %d", len(got), len(hostScope)+1)
	}
	// The host scope selector is the immutable StatefulSet selector as well - it must stay untouched
	if _, ok := hostScope[apps.StatefulSetPodNameLabel]; ok {
		t.Fatalf("host scope selector is modified")
	}
}
