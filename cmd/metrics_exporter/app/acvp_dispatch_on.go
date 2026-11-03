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

//go:build acvp_wrapper

package app

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/altinity/clickhouse-operator/pkg/util/fips/acvp"
)

// acvpArgv0Suffix is the suffix the binary's argv[0] must carry for the
// process to be treated as the ACVP responder rather than the metrics
// exporter. A CAVP/ACVP validation harness invokes the same exporter binary
// via a symlink or hardlink named "metrics-exporter-acvp"; this suffix is the
// trigger. Mirror of the operator-side constant in
// cmd/operator/app/acvp_dispatch_on.go so both binaries answer to the same
// "<name>-acvp" naming convention.
const acvpArgv0Suffix = "-acvp"

// TryACVPDispatch intercepts execution when the metrics-exporter binary was
// invoked under an "*-acvp" argv[0] (e.g. metrics-exporter-acvp) and runs the
// ACVP stdin/stdout responder in place of the normal exporter boot. Returns
// true only on a control-flow path that never actually returns: the responder
// reads JSON test vectors from stdin, writes JSON responses to stdout, then
// the function calls os.Exit so the exporter's flag.Parse / chop.Config /
// k8s client setup / Prometheus HTTP endpoint are never reached. Returns
// false (and the exporter boots normally) when argv[0] lacks the suffix.
//
// Build-tag gated: this file compiles only with -tags acvp_wrapper, which is
// how the validation build is produced. The default build picks up the stub
// in acvp_dispatch_off.go and never imports the acvp package, so the ACVP
// responder code is not present in production binaries. Structurally parallel
// to cmd/operator/app/acvp_dispatch_on.go; the two TryACVPDispatch functions
// live in different packages and do not share symbols, but their behavior and
// shape are kept identical so a single review of the operator binary covers
// the exporter binary too.
func TryACVPDispatch() bool {
	if !strings.HasSuffix(filepath.Base(os.Args[0]), acvpArgv0Suffix) {
		return false
	}
	acvp.Run(os.Stdin, os.Stdout)
	os.Exit(0)
	return true // unreachable; kept so the signature reads as a predicate
}
