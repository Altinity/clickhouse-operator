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

package fips

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEvaluateGate walks the chopconf × build × runtime matrix. Only the
// `enabled && !build` cell is fatal — chopconf asks for FIPS and the binary
// can't deliver. Strict runtime (`GODEBUG=fips140=only`, `runtime=true`) is
// informational, never required, per the operator's FIPS scope specification.
func TestEvaluateGate(t *testing.T) {
	const bin = "clickhouse-operator"
	cases := []struct {
		name           string
		enabled        bool
		build          bool
		runtime        bool
		defaultGODEBUG string
		wantErr        string
		wantWarn       string
	}{
		// Off — chopconf does not require FIPS; any binary/runtime is fine.
		{"off / unconfigured runtime", false, false, false, "", "", ""},
		{"off / FIPS-built only", false, true, false, "", "", ""},
		// Strict-runtime without chopconf agreement is a WARN — when the
		// runtime strictness comes from a user override (DefaultGODEBUG does
		// NOT contain fips140=only), the chopconf disagreement is real.
		{"off / GODEBUG=only without build, no baked default", false, false, true, "", "", "GODEBUG=fips140=only is set at runtime but security.fips.enforced is not set"},
		{"off / GODEBUG=only with build, no baked default", false, true, true, "", "", "GODEBUG=fips140=only is set at runtime but security.fips.enforced is not set"},
		// Strict-runtime that matches the binary-baked DefaultGODEBUG is the
		// documented shipped posture — suppress the warning.
		{"off / GODEBUG=only with build, baked default matches", false, true, true, "fips140=only", "", ""},
		{"off / GODEBUG=only with build, baked default (with siblings)", false, true, true, "asyncpreemptoff=1,fips140=only", "", ""},

		// Enabled — only the no-build cell errors.
		{"enabled / no build", true, false, false, "", "not built with GOFIPS140", ""},
		{"enabled / build, lenient runtime (default)", true, true, false, "", "", ""},
		{"enabled / build, strict runtime", true, true, true, "", "", ""},
		// Unreachable in production (Enforced requires Enabled) but pure-fn test
		// keeps the cell honest against future refactors.
		{"enabled / no build, strict runtime", true, false, true, "", "not built with GOFIPS140", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err, warn := EvaluateGate(bin, c.enabled, c.build, c.runtime, c.defaultGODEBUG)
			if c.wantErr != "" {
				require.Error(t, err, "expected gate to reject this matrix cell")
				require.Contains(t, err.Error(), c.wantErr)
				require.Contains(t, err.Error(), bin, "error must name the binary so the user knows which container to fix")
			} else {
				require.NoError(t, err)
			}
			if c.wantWarn != "" {
				require.Contains(t, warn, c.wantWarn)
			} else {
				require.Empty(t, warn)
			}
		})
	}
}
