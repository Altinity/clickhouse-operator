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

package runtime

import "testing"

func plainCaller() string {
	_, _, function := Caller("")
	return function
}

func genericCaller[T any](_ T) string {
	_, _, function := Caller("")
	return function
}

// TestCallerFunctionName covers https://github.com/Altinity/clickhouse-operator/issues/2101:
// a generic function was reported as "]".
func TestCallerFunctionName(t *testing.T) {
	if got := plainCaller(); got != "plainCaller" {
		t.Errorf("plain: got %q, want %q", got, "plainCaller")
	}
	if got := genericCaller(1); got != "genericCaller" {
		t.Errorf("generic: got %q, want %q", got, "genericCaller")
	}
}
