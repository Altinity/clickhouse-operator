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
	"crypto/fips140"
	"os"
	"runtime/debug"
)

// Indirection vars so tests can drive the gate down all matrix branches
// without controlling the Go toolchain. Production paths read
// crypto/fips140.Enabled / Enforced / Version; tests swap the function pointer.
var (
	Enabled  = fips140.Enabled
	Enforced = fips140.Enforced
	Version  = fips140.Version
)

// GODEBUGRaw returns the raw GODEBUG env var. Indirection seam so the FIPS
// banner can distinguish GODEBUG unset from fips140=on (both produce identical
// build.enabled=true runtime.enforced=false output otherwise).
var GODEBUGRaw = func() string { return os.Getenv("GODEBUG") }

// BuildSetting walks runtime/debug.ReadBuildInfo().Settings and returns the
// value for the given key (or "" when ReadBuildInfo fails or key is absent).
// Used to surface DefaultGODEBUG and GOFIPS140 in the FIPS banner.
var BuildSetting = func(key string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, s := range info.Settings {
		if s.Key == key {
			return s.Value
		}
	}
	return ""
}
