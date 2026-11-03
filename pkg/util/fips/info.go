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
	"io"
	"runtime"
)

// PrintInfo writes a human-readable FIPS build + runtime posture dump to w.
// Surfaces every field a customer or auditor would otherwise retrieve via
// `go version -m <binary>` (which requires a local Go toolchain) plus the
// live crypto/fips140 introspection that the startup banner already logs.
//
// Output is line-oriented `key: value` so tests can grep without a parser.
// Same content the FIPS startup banner emits, but on demand and without
// having to start the operator's full reconcile loop.
func PrintInfo(w io.Writer, binaryName, version, gitSHA, builtAt string) {
	fmt.Fprintf(w, "binary:          %s\n", binaryName)
	fmt.Fprintf(w, "version:         %s\n", version)
	fmt.Fprintf(w, "git_sha:         %s\n", gitSHA)
	fmt.Fprintf(w, "built_at:        %s\n", builtAt)
	fmt.Fprintf(w, "go_version:      %s\n", runtime.Version())
	fmt.Fprintf(w, "goos:            %s\n", runtime.GOOS)
	fmt.Fprintf(w, "goarch:          %s\n", runtime.GOARCH)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "fips_module:")
	fmt.Fprintf(w, "  build_setting: GOFIPS140=%q\n", BuildSetting("GOFIPS140"))
	fmt.Fprintf(w, "  enabled:       %t\n", Enabled())
	fmt.Fprintf(w, "  enforced:      %t\n", Enforced())
	fmt.Fprintf(w, "  version:       %s\n", Version())
	fmt.Fprintln(w)
	fmt.Fprintln(w, "godebug:")
	fmt.Fprintf(w, "  runtime_env:   %q\n", GODEBUGRaw())
	fmt.Fprintf(w, "  default:       %q\n", BuildSetting("DefaultGODEBUG"))
}
