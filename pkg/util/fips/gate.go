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
	"fmt"
	"strings"
)

// EvaluateGate decides what the FIPS startup gate should report for a given
// posture triple. Pure — no logging, no chop, no crypto/fips140 — so the
// chopconf × build × runtime matrix is table-testable in one place.
//
// Semantics per the operator's FIPS scope specification:
//   - `enabled` (chopconf knob `security.fips.enforced`): operator should run
//     in FIPS-compatible mode. Requires `build`=true (GOFIPS140-built binary).
//     The Go runtime mode (`GODEBUG=fips140=on` permissive vs `=only` strict,
//     exposed via `runtime`) is independent — `=on` is sufficient for FIPS
//     compatibility; `=only` is the shipped default (defense-in-depth).
//   - `runtime` (i.e. crypto/fips140.Enforced()): true iff strict mode
//     (`GODEBUG=fips140=only`) is active. Informational here; never required.
//   - `defaultGODEBUG`: the binary's DefaultGODEBUG build setting (set when
//     the linker baked a default via `//go:debug` or equivalent toolchain
//     config). When the image-baked default already selects `fips140=only`,
//     the runtime-strict state is the documented shipped posture, not a
//     configuration mismatch — suppress the warning in that case.
//
// Returns:
//   - err  != nil : caller must Fatal — chopconf asks for FIPS but the binary
//     can't deliver it (not GOFIPS140-built)
//   - warn != ""  : caller should Warning-log — strict runtime is set but
//     chopconf wants no FIPS, suggesting a configuration mismatch
//   - both empty  : posture is consistent; caller logs only the banner
//
// binaryName ("clickhouse-operator", "metrics-exporter", …) interpolates into
// the error string so the user sees which container is the misfit.
func EvaluateGate(binaryName string, enabled, build, runtime bool, defaultGODEBUG string) (err error, warn string) {
	switch {
	case enabled && !build:
		return fmt.Errorf("FIPS: chopconf security.fips.enforced=true but the %s binary was not built with GOFIPS140 — rebuild with GOFIPS140=v1.0.0 or set security.fips.enforced=false", binaryName), ""
	case !enabled && runtime && !defaultGODEBUGHasFipsOnly(defaultGODEBUG):
		return nil, "FIPS: GODEBUG=fips140=only is set at runtime but security.fips.enforced is not set — strict mode is active despite chopconf disagreement"
	}
	return nil, ""
}

// defaultGODEBUGHasFipsOnly reports whether the binary-baked DefaultGODEBUG
// already selects `fips140=only`, i.e. the strict runtime is the shipped
// posture rather than a user override.
func defaultGODEBUGHasFipsOnly(defaultGODEBUG string) bool {
	for _, kv := range strings.Split(defaultGODEBUG, ",") {
		if strings.TrimSpace(kv) == "fips140=only" {
			return true
		}
	}
	return false
}
