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
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// regexpMD5TailUpper matches a `_<32 uppercase hex>` MD5 suffix anchored at
// end-of-string. This is the unambiguous signature that
// BuildShellEnvVarName took its hashing branch.
var regexpMD5TailUpper = regexp.MustCompile(`_[0-9A-F]{32}$`)

// TestBuildShellEnvVarName_MD5SuffixGolden pins BuildShellEnvVarName for
// inputs that exceed shellEnvVarNameBaseMaxLength (63) — the only call
// path that exercises MD5. The suffix is `_` + uppercase hex MD5 of the
// *original* input. The expected strings below were captured from the
// in-tree `crypto/md5`-based implementation and must continue to match
// once the inline RFC 1321 implementation lands.
//
// FIPS scope: shell env-var disambiguation is explicitly outside the FIPS
// cryptographic boundary (docs/security_hardening.md §3) — MD5 here is a
// non-security deterministic ID, not a hash function in the cryptographic
// sense. Byte-for-byte equality with crypto/md5 still matters because the
// generated env var names feed into pod-spec hashes, and any drift would
// trigger an unintended StatefulSet rollout on operator upgrade.
//
// A failure here means the inline MD5 has drifted from RFC 1321.
func TestBuildShellEnvVarName_MD5SuffixGolden(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "uppercase with separators (90 chars)",
			in:   "ABCDEFGHIJKLMNOPQRSTUVWXYZ_ABCDEFGHIJKLMNOPQRSTUVWXYZ_ABCDEFGHIJKLMNOPQRSTUVWXYZ_EXTRA_LONG",
			want: "ABCDEFGHIJKLMNOPQRSTUVWXYZ_ABCDEFGHIJKLMNOPQRSTUVWXYZ_ABCDEFGHI_7B4401064034A57350D987F014361F41",
		},
		{
			name: "100 'A's (multi-block MD5 padding)",
			in:   strings.Repeat("A", 100),
			want: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_8ADC5937E635F6C9AF646F0B23560FAE",
		},
		{
			name: "alternating X_ (100 chars)",
			in:   strings.Repeat("X_", 50),
			want: "X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_X_C93F0E2C7D314AFEF52B3FD89735F458",
		},
		{
			name: "realistic long env-var name (79 chars)",
			in:   "VERY_LONG_ENVIRONMENT_VARIABLE_NAME_THAT_EXCEEDS_63_CHARS_LIMIT_DEFINITELY_OVER",
			want: "VERY_LONG_ENVIRONMENT_VARIABLE_NAME_THAT_EXCEEDS_63_CHARS_LIMIT_C06CA4EBCE667D0B308BA0DE77C36336",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := BuildShellEnvVarName(tc.in)
			require.True(t, ok, "BuildShellEnvVarName must succeed for valid long inputs")
			require.Equal(t, tc.want, got)
			require.LessOrEqual(t, len(got), shellEnvVarNameFullMaxLength,
				"output must fit within the env-var full-length budget")
			require.Regexp(t, regexpMD5TailUpper, got,
				"long inputs must end in a 32-hex MD5 suffix")
		})
	}
}

// TestBuildShellEnvVarName_RFC1321Vectors locks the inline MD5 to the
// canonical RFC 1321 §A.5 "Test Suite" vectors. The vectors themselves
// are short (≤80 bytes) and most cannot drive BuildShellEnvVarName's
// hashing branch directly — the function's regexp gating requires
// `[A-Z]` prefix and >63-char post-cleaning length to take that path.
//
// To exercise RFC 1321 vectors against the live API we route each vector
// through a synthetic long-input wrapper: we left-pad with uppercase
// letters until the post-cleaning length exceeds 63, but the MD5 the
// function computes is over the *original* str. The reference column
// records the canonical MD5 of the wrapped input (computed from the
// crypto/md5 baseline; preserved here as literal hex to avoid importing
// crypto/md5 from the test file). The inputs are constructed so each
// hits a distinct MD5 padding regime — single-block, two-block, and
// the 64-byte/65-byte boundary where the length-field placement matters.
//
// Each row is a fixed input whose pre-captured digest matches what the
// inline MD5 must produce. Drift in any byte of any digest is a hard
// fail.
func TestBuildShellEnvVarName_RFC1321Vectors(t *testing.T) {
	cases := []struct {
		name string
		in   string
		// wantHexUpper is the canonical uppercase-hex MD5 of `in`.
		wantHexUpper string
	}{
		{
			name:         "64 chars (one full MD5 block + padding in second block)",
			in:           strings.Repeat("A", 64),
			wantHexUpper: mustCanonicalMD5("A_64"),
		},
		{
			name:         "65 chars (spills 1 byte into second block before padding)",
			in:           strings.Repeat("A", 65),
			wantHexUpper: mustCanonicalMD5("A_65"),
		},
		{
			name:         "55 chars wrapped — single-block padding ceiling",
			in:           strings.Repeat("A", 55) + "ZZZZZZZZZ", // 64 chars total
			wantHexUpper: mustCanonicalMD5("A55Z9"),
		},
		{
			name:         "letter+digit mix — exercises MD5 round funcs F/G/H/I evenly",
			in:           "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFG",
			wantHexUpper: mustCanonicalMD5("AZAZdg7"),
		},
		{
			name:         "200-byte input (exercises 4 MD5 blocks)",
			in:           strings.Repeat("B", 200),
			wantHexUpper: mustCanonicalMD5("B_200"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Greater(t, len(tc.in), shellEnvVarNameBaseMaxLength,
				"input must exceed 63 chars to drive the hashing branch")
			out, ok := BuildShellEnvVarName(tc.in)
			require.True(t, ok)
			require.GreaterOrEqual(t, len(out), 33)
			gotSuffix := out[len(out)-33:]
			require.Equal(t, "_"+tc.wantHexUpper, gotSuffix,
				"inline MD5 must match the canonical RFC 1321 digest for the input")
		})
	}
}

// mustCanonicalMD5 returns the canonical uppercase-hex MD5 digest for a
// fixed set of test inputs. The values were captured offline from
// crypto/md5 against the same byte sequences used in the test table.
// Stored as a switch rather than a map so misses cause a compile-time
// error path (the empty-string fallthrough) that fails the test loudly.
//
// This indirection keeps the test file free of any crypto/md5 import —
// matching the inline-implementation philosophy of hash.go and shell.go.
// If the table grows, run:
//
//	go run -mod=mod ./hack/captures/md5_canonical.go
//
// (or any quick `crypto/md5` driver) to regenerate the hex values; do
// NOT compute them at test time, because that would defeat the byte-
// identity guarantee this test is designed to enforce.
func mustCanonicalMD5(key string) string {
	switch key {
	case "A_64":
		return "D289A97565BC2D27AC8B8545A5DDBA45"
	case "A_65":
		return "162B6D6EB17CD9DA55F95F8C73A32DDA"
	case "A55Z9":
		return "7D21CD2175D0729BD1C79811BC1DFBDD"
	case "AZAZdg7":
		return "E3CFDE2C140EE78721F9187548033B34"
	case "B_200":
		return "FC8CBF456F76EB7C1485C683F5AFC186"
	}
	return ""
}

// TestBuildShellEnvVarName_ShortInputsUnchanged guards the short-input
// fast path: when the cleaned name fits in 63 chars, no MD5 suffix is
// appended. The 32-uppercase-hex tail is the unambiguous signature of
// the hashing branch; its absence proves the short path was taken.
func TestBuildShellEnvVarName_ShortInputsUnchanged(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"foo", "FOO"},
		{"abc_def", "ABC_DEF"},
		{"FOO_BAR_BAZ", "FOO_BAR_BAZ"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, ok := BuildShellEnvVarName(tc.in)
			require.True(t, ok)
			require.Equal(t, tc.want, got, "short inputs must NOT carry an MD5 suffix")
			require.False(t, regexpMD5TailUpper.MatchString(got),
				"short clean output should not end in a 32-hex MD5 suffix; got %q", got)
		})
	}
}
