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

package util

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/util/fips"
)

// The tests in this file verify that the operator's identifier-derivation
// code paths (HashIntoString + BuildShellEnvVarName) DO NOT panic when the
// Go runtime is started with `GODEBUG=fips140=only` — a mode under which
// any reference to crypto/sha1 or crypto/md5 fires runtime.Throw at
// package init or first use.
//
// Both digest helpers under test are intentionally inlined (no crypto/sha1
// or crypto/md5 imports anywhere in the package) precisely so the operator
// stays loadable under strict mode while still emitting the byte-identical
// fingerprints that K8s object-version labels and shell-env-var-name
// suffixes rely on for stability across upgrades.
//
// HOW TO RUN UNDER STRICT MODE:
//
//	GOFIPS140=v1.0.0 GODEBUG=fips140=only go test -count=1 \
//	    -run 'TestHashIntoString_NoPanicUnderFIPS140Only|TestBuildShellEnvVarName_NoPanicUnderFIPS140Only|TestCryptoFipsModeInfo' \
//	    ./pkg/util/...
//
// GODEBUG MUST be set in the parent environment of `go test` — the runtime
// reads it at process init, so `t.Setenv("GODEBUG", "fips140=only")` from
// inside a test has no effect.
//
// The tests are unconditional (no build tags) so `go test ./...` runs them
// by default; they SKIP gracefully when GODEBUG does not request strict
// mode, so the normal CI matrix is unaffected.

// fipsStrictModeRequested reports whether the parent shell asked for
// strict FIPS mode via GODEBUG. The runtime parses GODEBUG itself; this
// helper merely mirrors that parse so the test can decide whether the
// no-panic guarantee is meaningful in the current environment.
func fipsStrictModeRequested() bool {
	for _, kv := range strings.Split(os.Getenv("GODEBUG"), ",") {
		if strings.TrimSpace(kv) == "fips140=only" {
			return true
		}
	}
	return false
}

// TestHashIntoString_NoPanicUnderFIPS140Only asserts that HashIntoString
// returns the canonical SHA-1 digest of "abc" (FIPS PUB 180-4 §A.1 test
// vector) without panicking, even when the toolchain is in strict mode.
// A panic here means somebody re-imported crypto/sha1 into pkg/util/hash.go
// — revert that change before merging.
func TestHashIntoString_NoPanicUnderFIPS140Only(t *testing.T) {
	if !fipsStrictModeRequested() {
		t.Skip("fips140only_test: only meaningful under GODEBUG=fips140=only; got GODEBUG=" + os.Getenv("GODEBUG"))
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("HashIntoString panicked under GODEBUG=fips140=only: %v — a crypto/sha1 import has been reintroduced into pkg/util", r)
		}
	}()

	// FIPS PUB 180-4 §A.1 / RFC 3174 §7.3 known-answer: SHA-1("abc").
	const want = "a9993e364706816aba3e25717850c26c9cd0d89d"
	got := HashIntoString([]byte("abc"))
	require.Equal(t, want, got, "HashIntoString must produce byte-identical SHA-1 of 'abc' (FIPS 180-4 §A.1 test vector); diverging means object-version labels would churn across upgrade")
}

// TestBuildShellEnvVarName_NoPanicUnderFIPS140Only forces the MD5
// uniqueness-suffix branch by feeding a 100-char input (longer than the
// 63-char base limit) and asserts the uppercase-hex MD5 tail matches the
// canonical RFC 1321 §A.5 known-answer.
func TestBuildShellEnvVarName_NoPanicUnderFIPS140Only(t *testing.T) {
	if !fipsStrictModeRequested() {
		t.Skip("fips140only_test: only meaningful under GODEBUG=fips140=only; got GODEBUG=" + os.Getenv("GODEBUG"))
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("BuildShellEnvVarName panicked under GODEBUG=fips140=only: %v — a crypto/md5 import has been reintroduced into pkg/util", r)
		}
	}()

	// 100-char alphanumeric input — survives the env-var-name sanitizer
	// unchanged (it is already valid [A-Z0-9]+) and exceeds the 63-char
	// base limit, which is what forces the MD5-suffix branch in
	// BuildShellEnvVarName to fire.
	input := strings.Repeat("ABC", 33) + "Z" // 3*33 + 1 = 100 chars
	require.Equal(t, 100, len(input), "test precondition: input must be 100 chars to trigger the digest path")

	name, ok := BuildShellEnvVarName(input)
	require.True(t, ok, "BuildShellEnvVarName must succeed for a clean 100-char alphanumeric input")
	require.LessOrEqual(t, len(name), shellEnvVarNameFullMaxLength, "full name must stay within the shell env-var-name limit")

	// Base prefix = uppercase input truncated to 63 chars, no trailing '_'.
	wantPrefix := strings.ToUpper(input)[:shellEnvVarNameBaseMaxLength]
	require.True(t, strings.HasPrefix(name, wantPrefix+"_"), "expected name to start with the 63-char prefix followed by the suffix delimiter; got %q", name)

	// Suffix = "_" + uppercase hex MD5 of the ORIGINAL input (32 hex chars).
	suffix := name[len(wantPrefix)+1:]
	require.Len(t, suffix, 32, "MD5 hex suffix must be exactly 32 chars")
	require.Regexp(t, "^[0-9A-F]{32}$", suffix, "suffix must be uppercase hex")

	// Independent reference value: MD5(input) computed with `md5sum` and
	// pinned here as the known-answer. Diverging means the inlined MD5 has
	// been altered and existing env-var names would drift on upgrade.
	const wantMD5OfRepeatedABC = "DECDFC0799C993606308511B5B19148F"
	require.Equal(t, wantMD5OfRepeatedABC, suffix, "MD5 hex suffix must be byte-identical to RFC 1321 MD5 of the input; diverging means env-var names would churn across upgrade")
}

// TestCryptoFipsModeInfo prints what crypto/fips140 reports about the
// current process — purely informational. Helps a human verify that the
// strict-mode test invocation is actually being honored by the toolchain
// (i.e. that GOFIPS140 + GODEBUG were both set in the parent env).
// Always runs; never fails.
func TestCryptoFipsModeInfo(t *testing.T) {
	t.Logf("fips140.Enabled()  = %v", fips.Enabled())
	t.Logf("fips140.Enforced() = %v", fips.Enforced())
	t.Logf("fips140.Version()  = %q", fips.Version())
	t.Logf("os.Getenv(\"GODEBUG\")    = %q", os.Getenv("GODEBUG"))
	t.Logf("os.Getenv(\"GOFIPS140\")  = %q", os.Getenv("GOFIPS140"))
	t.Logf("fipsStrictModeRequested() = %v", fipsStrictModeRequested())
}
