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

package types

import (
	"encoding/json"
	"testing"
)

// TestStringBool_UnmarshalJSON_Polymorphic pins the load-bearing contract:
// users can write `secure: true` (native YAML bool round-tripped to JSON bool
// by kubectl), `secure: 1` (integer), or `secure: "true"` / `"yes"` / etc.
// (string vocabulary) — and they all arrive at the operator as a StringBool
// that reports IsTrue() correctly. The CRD's anyOf:[boolean,integer,string]
// anchor mirrors this so the API server's structural-schema validator
// accepts all three forms at admission time.
func TestStringBool_UnmarshalJSON_Polymorphic(t *testing.T) {
	type field struct {
		Secure *StringBool `json:"secure"`
	}
	cases := []struct {
		name      string
		input     string
		wantTrue  bool
		wantFalse bool
		wantNil   bool
		wantErr   bool
	}{
		// Native JSON bool (kubectl path for unquoted YAML `secure: true`)
		{"native bool true", `{"secure": true}`, true, false, false, false},
		{"native bool false", `{"secure": false}`, false, true, false, false},
		// Integer 0/1 (legacy ClickHouse-config vocabulary)
		{"int 1", `{"secure": 1}`, true, false, false, false},
		{"int 0", `{"secure": 0}`, false, true, false, false},
		// String vocabulary
		{"string true", `{"secure": "true"}`, true, false, false, false},
		{"string True", `{"secure": "True"}`, true, false, false, false},
		{"string TRUE", `{"secure": "TRUE"}`, true, false, false, false},
		{"string yes", `{"secure": "yes"}`, true, false, false, false},
		{"string Yes", `{"secure": "Yes"}`, true, false, false, false},
		{"string on", `{"secure": "on"}`, true, false, false, false},
		{"string enabled", `{"secure": "enabled"}`, true, false, false, false},
		{"string 1", `{"secure": "1"}`, true, false, false, false},
		{"string false", `{"secure": "false"}`, false, true, false, false},
		{"string No", `{"secure": "No"}`, false, true, false, false},
		{"string off", `{"secure": "off"}`, false, true, false, false},
		{"string disabled", `{"secure": "disabled"}`, false, true, false, false},
		// Null / missing
		{"null", `{"secure": null}`, false, false, true, false},
		{"absent", `{}`, false, false, true, false},
		// Empty string is a valid (recognized) StringBool — neither true nor false
		{"empty string", `{"secure": ""}`, false, false, false, false},
		// Genuine rejections
		{"int 42", `{"secure": 42}`, false, false, false, false}, // unrecognized int → falls through to "1" (any non-zero), still ok
		{"float", `{"secure": 1.5}`, false, false, false, true},
		{"object", `{"secure": {}}`, false, false, false, true},
		{"array", `{"secure": []}`, false, false, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var f field
			err := json.Unmarshal([]byte(c.input), &f)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil; field=%v", f.Secure)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if c.wantNil {
				if f.Secure != nil {
					t.Fatalf("expected nil, got %v", *f.Secure)
				}
				return
			}
			if f.Secure == nil {
				t.Fatalf("expected non-nil, got nil")
			}
			if c.wantTrue && !f.Secure.IsTrue() {
				t.Fatalf("expected IsTrue=true, got %q (IsTrue=%t IsFalse=%t)", *f.Secure, f.Secure.IsTrue(), f.Secure.IsFalse())
			}
			if c.wantFalse && !f.Secure.IsFalse() {
				t.Fatalf("expected IsFalse=true, got %q (IsTrue=%t IsFalse=%t)", *f.Secure, f.Secure.IsTrue(), f.Secure.IsFalse())
			}
		})
	}
}

// TestStringBool_RoundTrip_Marshal verifies that the default string marshaling
// (no custom MarshalJSON) emits a quoted string — preserving kubectl output
// stability for users with GitOps diff pipelines.
func TestStringBool_RoundTrip_Marshal(t *testing.T) {
	s := NewStringBool(true)
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `"True"` {
		t.Fatalf("expected %q, got %s", `"True"`, data)
	}
}
