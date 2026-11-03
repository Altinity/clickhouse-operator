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
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHashIntoString pins the externally-visible contract of HashIntoString:
// 40-char lowercase hex output (K8s label-value-safe; ≤63 chars rule), empty
// input returns empty, and the function is deterministic. The 40-char width
// is load-bearing — the output is written to
// `clickhouse.altinity.com/object-version` labels via MakeObjectVersion, and
// a width change would invalidate every label-value comparison across an
// operator upgrade. Failing this test means the algorithm has been swapped
// to a different hash or truncation length: review the K8s-label-width
// implication before touching.
func TestHashIntoString(t *testing.T) {
	require.Equal(t, "", HashIntoString(nil), "empty input must return empty string")
	require.Equal(t, "", HashIntoString([]byte{}), "zero-length input must return empty string")

	out := HashIntoString([]byte("any non-empty input"))
	require.Len(t, out, 40, "output width is part of the contract — K8s label values must be ≤63")
	require.Regexp(t, "^[0-9a-f]{40}$", out, "output must be lowercase hex")

	// Determinism.
	require.Equal(t, out, HashIntoString([]byte("any non-empty input")))

	// Different inputs produce different outputs.
	require.NotEqual(t, out, HashIntoString([]byte("another input")))
}

// TestObjectVersionDigest_RFC3174Vectors locks objectVersionDigest to the
// canonical SHA-1 test vectors from FIPS PUB 180-4 / RFC 3174 Appendix A.
// The inline implementation (hash.go) replaces a former `crypto/sha1`
// dependency so the operator can run under `GODEBUG=fips140=only`. The
// digest is byte-for-byte stable across that swap — any drift here means
// every existing `clickhouse.altinity.com/object-version` label would change
// on upgrade, triggering a needless StatefulSet rollout fleet-wide.
//
// Expected values are literal hex strings; the test file deliberately does
// NOT import crypto/sha1 so a regression in the inline algorithm cannot be
// masked by accidentally re-comparing against the stdlib digest.
func TestObjectVersionDigest_RFC3174Vectors(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want string // lowercase hex of 20-byte digest
	}{
		{
			name: "empty string (FIPS 180-4 Appendix A.1 corner case)",
			in:   []byte(""),
			want: "da39a3ee5e6b4b0d3255bfef95601890afd80709",
		},
		{
			name: "abc (RFC 3174 Appendix A TEST1)",
			in:   []byte("abc"),
			want: "a9993e364706816aba3e25717850c26c9cd0d89d",
		},
		{
			name: "56-byte multi-block (RFC 3174 Appendix A TEST2)",
			in:   []byte("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
			want: "84983e441c3bd26ebaae4aa1f95129e5e54670f1",
		},
		{
			name: "one million 'a' (RFC 3174 Appendix A TEST3)",
			in:   []byte(strings.Repeat("a", 1000000)),
			want: "34aa973cd4c4daa4f61eeb2bdbad27316534016f",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := hex.EncodeToString(objectVersionDigest(tc.in))
			require.Equal(t, tc.want, got)
		})
	}
}

// TestHashIntoString_RFC3174Vectors verifies the public-API surface
// (HashIntoString) yields the canonical SHA-1 hex string for each non-empty
// RFC 3174 vector. Empty input has its own contract (returns ""), checked
// separately in TestHashIntoString.
func TestHashIntoString_RFC3174Vectors(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		{
			name: "abc",
			in:   []byte("abc"),
			want: "a9993e364706816aba3e25717850c26c9cd0d89d",
		},
		{
			name: "56-byte multi-block",
			in:   []byte("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
			want: "84983e441c3bd26ebaae4aa1f95129e5e54670f1",
		},
		{
			name: "one million 'a'",
			in:   []byte(strings.Repeat("a", 1000000)),
			want: "34aa973cd4c4daa4f61eeb2bdbad27316534016f",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, HashIntoString(tc.in))
		})
	}
}

// TestObjectVersionDigest_PaddingBoundaries exercises the three input lengths
// that drive the SHA-1 padding state machine into distinct branches:
//
//   - 55 bytes: last block fits message + 0x80 + length (single-block padding)
//   - 56 bytes: 0x80 + length no longer fits → forces a second padding block
//   - 64 bytes: full block + 0x80 forces a second block (only length fits there)
//   - 65 bytes: spills into a second block before padding even starts
//
// These are the off-by-one spots where a naive inline implementation goes
// wrong: padded-length arithmetic, block-count loop bounds, big-endian length
// field placement. Expected digests are independently sourced canonical
// values (any standard sha1sum on the byte sequences below reproduces them).
func TestObjectVersionDigest_PaddingBoundaries(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		{
			name: "55 bytes — single-block padding ceiling",
			in:   bytes55(),
			want: "c1c8bbdc22796e28c0e15163d20899b65621d65a",
		},
		{
			name: "56 bytes — forces 2-block padding",
			in:   bytes56(),
			want: "c2db330f6083854c99d4b5bfb6e8f29f201be699",
		},
		{
			name: "64 bytes — exact block, padding goes to next block",
			in:   bytes64(),
			want: "0098ba824b5c16427bd7a1122a5a442a25ec644d",
		},
		{
			name: "65 bytes — spills 1 byte into second block",
			in:   bytes65(),
			want: "11655326c708d70319be2610e8a57d9a5b959d3b",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Len(t, tc.in, lenFromName(tc.name))
			got := hex.EncodeToString(objectVersionDigest(tc.in))
			require.Equal(t, tc.want, got)
		})
	}
}

// Helpers — fixed-length inputs whose canonical SHA-1 digests are pinned in
// TestObjectVersionDigest_PaddingBoundaries. Using a single repeated byte
// keeps the inputs reproducible by any external sha1sum check; the digests
// above were captured from a known-good crypto/sha1 baseline before the
// inline implementation landed.
func bytes55() []byte { return []byte(strings.Repeat("a", 55)) }
func bytes56() []byte { return []byte(strings.Repeat("a", 56)) }
func bytes64() []byte { return []byte(strings.Repeat("a", 64)) }
func bytes65() []byte { return []byte(strings.Repeat("a", 65)) }

// lenFromName parses the leading integer from a test-case name like
// "55 bytes — ...". Keeps the table self-validating: if a future edit
// shortens or lengthens an input by mistake, the require.Len trips before
// we ever compare digests.
func lenFromName(name string) int {
	n := 0
	for i := 0; i < len(name) && name[i] >= '0' && name[i] <= '9'; i++ {
		n = n*10 + int(name[i]-'0')
	}
	return n
}
