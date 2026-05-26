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
	"encoding/binary"
	"encoding/hex"
	"math/bits"
	"regexp"
	"strings"
)

const shellEnvVarNameBaseMaxLength int = 63
const shellEnvVarNameFullMaxLength int = 127

var shellEnvVarNameRegexp = regexp.MustCompile("^[A-Z]([_A-Z0-9]*[A-Z0-9])?$")
var shellEnvVarNameStartRegexp = regexp.MustCompile("^[A-Z]")
var shellEnvVarNameNotAllowedCharsRegexp = regexp.MustCompile("[^_A-Z0-9]")
var shellEnvVarNameReplaceCharsRegexp = regexp.MustCompile("[/]")

func BuildShellEnvVarName(str string) (name string, ok bool) {
	// Do not touch original value
	name = str

	// Must be uppercase
	name = strings.ToUpper(name)

	// First char must comply to start regexp
	// Cut the first char until it is reasonable
	for len(name) > 0 {
		if shellEnvVarNameStartRegexp.MatchString(name) {
			break
		} else {
			name = name[1:]
		}
	}

	// Replace replaceable chars
	name = shellEnvVarNameReplaceCharsRegexp.ReplaceAllString(name, "_")
	// Remove not allowed chars
	name = shellEnvVarNameNotAllowedCharsRegexp.ReplaceAllString(name, "")

	// Must have limited length
	suffix := ""
	if len(name) > shellEnvVarNameBaseMaxLength {
		// Cut the name
		name = name[0:shellEnvVarNameBaseMaxLength]
		// Non-cryptographic uniqueness suffix keeps the total length within
		// shellEnvVarNameFullMaxLength while preserving determinism across
		// reconciles (same input always yields the same env-var name).
		digest := envVarNameSuffixDigest([]byte(str))
		suffix = "_" + strings.ToUpper(hex.EncodeToString(digest[:]))
	}

	// Ensure no trailing underscores
	name = strings.TrimRight(name, "_")

	// Append suffix to keep name uniqueness
	name += suffix

	// It still has to be a valid env ma,e after all
	if IsShellEnvVarName(name) {
		return name, true
	}

	return "", false
}

// IsShellEnvVarName tests for a string that conforms to the definition of a shell ENV VAR name
func IsShellEnvVarName(value string) bool {
	if len(value) > shellEnvVarNameFullMaxLength {
		return false
	}
	if !shellEnvVarNameRegexp.MatchString(value) {
		return false
	}
	return true
}

// envVarNameSuffixDigest computes a 16-byte deterministic digest used solely
// as a uniqueness suffix when shell env-var names exceed the base length
// limit. The output is byte-identical to RFC 1321 MD5 so that env-var names
// remain stable across operator versions (no StatefulSet rollout induced by
// suffix drift).
//
// This is an object/name-shortening function, NOT cryptographic protection.
// MD5 is inlined here (rather than imported from crypto/md5) so the operator
// can run under GODEBUG=fips140=only without panicking when this code path
// fires. See docs/security_hardening.md §3 — env-var suffix generation is
// explicitly outside the FIPS cryptographic boundary.
func envVarNameSuffixDigest(data []byte) [16]byte {
	// RFC 1321 §3.3 — initial chaining values (little-endian words A,B,C,D).
	a := uint32(0x67452301)
	b := uint32(0xEFCDAB89)
	c := uint32(0x98BADCFE)
	d := uint32(0x10325476)

	// Pad per RFC 1321 §3.1: append 0x80, then zeros, so total length ≡ 56 mod 64,
	// then append the original bit length as a 64-bit little-endian integer.
	msgBitLen := uint64(len(data)) * 8
	padded := make([]byte, 0, len(data)+72)
	padded = append(padded, data...)
	padded = append(padded, 0x80)
	for len(padded)%64 != 56 {
		padded = append(padded, 0x00)
	}
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], msgBitLen)
	padded = append(padded, lenBuf[:]...)

	// Process each 512-bit block.
	for off := 0; off < len(padded); off += 64 {
		var x [16]uint32
		for i := 0; i < 16; i++ {
			// MD5 is little-endian (contrast: SHA-1 is big-endian).
			x[i] = binary.LittleEndian.Uint32(padded[off+i*4 : off+i*4+4])
		}
		a, b, c, d = md5BlockRounds(a, b, c, d, &x)
	}

	var out [16]byte
	binary.LittleEndian.PutUint32(out[0:4], a)
	binary.LittleEndian.PutUint32(out[4:8], b)
	binary.LittleEndian.PutUint32(out[8:12], c)
	binary.LittleEndian.PutUint32(out[12:16], d)
	return out
}

// md5RoundTable holds T[i] = floor(2^32 * |sin(i+1)|) per RFC 1321 §3.4.
var md5RoundTable = [64]uint32{
	0xD76AA478, 0xE8C7B756, 0x242070DB, 0xC1BDCEEE,
	0xF57C0FAF, 0x4787C62A, 0xA8304613, 0xFD469501,
	0x698098D8, 0x8B44F7AF, 0xFFFF5BB1, 0x895CD7BE,
	0x6B901122, 0xFD987193, 0xA679438E, 0x49B40821,
	0xF61E2562, 0xC040B340, 0x265E5A51, 0xE9B6C7AA,
	0xD62F105D, 0x02441453, 0xD8A1E681, 0xE7D3FBC8,
	0x21E1CDE6, 0xC33707D6, 0xF4D50D87, 0x455A14ED,
	0xA9E3E905, 0xFCEFA3F8, 0x676F02D9, 0x8D2A4C8A,
	0xFFFA3942, 0x8771F681, 0x6D9D6122, 0xFDE5380C,
	0xA4BEEA44, 0x4BDECFA9, 0xF6BB4B60, 0xBEBFBC70,
	0x289B7EC6, 0xEAA127FA, 0xD4EF3085, 0x04881D05,
	0xD9D4D039, 0xE6DB99E5, 0x1FA27CF8, 0xC4AC5665,
	0xF4292244, 0x432AFF97, 0xAB9423A7, 0xFC93A039,
	0x655B59C3, 0x8F0CCC92, 0xFFEFF47D, 0x85845DD1,
	0x6FA87E4F, 0xFE2CE6E0, 0xA3014314, 0x4E0811A1,
	0xF7537E82, 0xBD3AF235, 0x2AD7D2BB, 0xEB86D391,
}

// md5BlockRounds runs the four 16-step rounds of RFC 1321 §3.4 over one block.
func md5BlockRounds(a, b, c, d uint32, x *[16]uint32) (uint32, uint32, uint32, uint32) {
	// Per-round shift amounts (RFC 1321 §3.4).
	s1 := [4]uint32{7, 12, 17, 22}
	s2 := [4]uint32{5, 9, 14, 20}
	s3 := [4]uint32{4, 11, 16, 23}
	s4 := [4]uint32{6, 10, 15, 21}

	aa, bb, cc, dd := a, b, c, d

	// Round 1: F(x,y,z) = (x AND y) OR (NOT x AND z); message-word index = i.
	for i := 0; i < 16; i++ {
		f := (b & c) | (^b & d)
		a, b, c, d = d, b+bits.RotateLeft32(a+f+x[i]+md5RoundTable[i], int(s1[i%4])), b, c
	}
	// Round 2: G(x,y,z) = (x AND z) OR (y AND NOT z); index = (5i+1) mod 16.
	for i := 0; i < 16; i++ {
		g := (b & d) | (c & ^d)
		k := (5*i + 1) % 16
		a, b, c, d = d, b+bits.RotateLeft32(a+g+x[k]+md5RoundTable[16+i], int(s2[i%4])), b, c
	}
	// Round 3: H(x,y,z) = x XOR y XOR z; index = (3i+5) mod 16.
	for i := 0; i < 16; i++ {
		h := b ^ c ^ d
		k := (3*i + 5) % 16
		a, b, c, d = d, b+bits.RotateLeft32(a+h+x[k]+md5RoundTable[32+i], int(s3[i%4])), b, c
	}
	// Round 4: I(x,y,z) = y XOR (x OR NOT z); index = (7i) mod 16.
	for i := 0; i < 16; i++ {
		ii := c ^ (b | ^d)
		k := (7 * i) % 16
		a, b, c, d = d, b+bits.RotateLeft32(a+ii+x[k]+md5RoundTable[48+i], int(s4[i%4])), b, c
	}

	return a + aa, b + bb, c + cc, d + dd
}
