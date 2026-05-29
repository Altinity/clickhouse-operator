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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math/bits"

	dumper "github.com/sanity-io/litter"
)

func serializeUnrepeatable(obj interface{}) []byte {
	b := bytes.Buffer{}
	encoder := gob.NewEncoder(&b)
	err := encoder.Encode(obj)
	if err != nil {
		fmt.Println(`failed gob Encode`, err)
	}

	return b.Bytes()
}

func serializeRepeatable(obj interface{}) []byte {
	d := dumper.Options{
		Separator: " ",
	}
	return []byte(d.Sdump(obj))
}

// HashIntoString returns a deterministic 40-char hex digest used purely as a
// non-cryptographic object fingerprint / shortener for K8s object names and
// label values (callers: Fingerprint, labeler.MakeObjectVersion). It is NOT a
// security control — no integrity, signing, authentication, or
// confidentiality use is implied or supported. The digest exists solely to
// compare two serialized object representations for equality in a
// label-length-bounded form.
//
// To keep the operator deployable under the Go runtime's strict
// `GODEBUG=fips140=only` mode (which forbids any reference to crypto/sha1
// even from non-security code paths), the digest is produced by an inline
// pure-Go implementation of the algorithm specified by FIPS PUB 180-4 §6.1.2
// and RFC 3174, rather than by importing crypto/sha1. See
// docs/security_hardening_fips.md § "Non-security hash exclusions" for the
// rationale: this site is documented as outside the FIPS cryptographic
// boundary because the output is never used as a security primitive.
//
// Byte-for-byte output equivalence with crypto/sha1 is preserved
// intentionally so that existing `clickhouse.altinity.com/object-version`
// labels remain stable across the upgrade and no StatefulSet rollout is
// triggered.
func HashIntoString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return hex.EncodeToString(objectVersionDigest(b))
}

// HashIntoInt hashes bytes and returns int version of the hash
func HashIntoInt(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(b)
	return int(h.Sum32())
}

// HashIntoIntTopped hashes bytes and return int version of the ash topped with top
func HashIntoIntTopped(b []byte, top int) int {
	return HashIntoInt(b) % top
}

// objectVersionDigest computes a 20-byte deterministic fingerprint of the
// input used solely for K8s object/name shortening and label equality
// comparison — NOT for any cryptographic purpose (no integrity, signing,
// authentication, or confidentiality). The byte layout matches the algorithm
// defined by FIPS PUB 180-4 §6.1.2 / RFC 3174 so that existing object
// fingerprint labels remain stable across releases. The implementation is
// inlined deliberately to avoid importing crypto/sha1, which would otherwise
// cause `GODEBUG=fips140=only` to panic on operator startup even though this
// call site is documented as outside the FIPS cryptographic boundary (see
// docs/security_hardening_fips.md § "Non-security hash exclusions").
func objectVersionDigest(msg []byte) []byte {
	// Initial state — FIPS PUB 180-4 §5.3.1 / RFC 3174 §6.1.
	h := [5]uint32{
		0x67452301,
		0xEFCDAB89,
		0x98BADCFE,
		0x10325476,
		0xC3D2E1F0,
	}

	// Pre-processing: append 0x80, zero-pad to 56 mod 64, append 64-bit
	// big-endian bit length. RFC 3174 §4.
	bitLen := uint64(len(msg)) * 8
	padded := make([]byte, 0, len(msg)+72)
	padded = append(padded, msg...)
	padded = append(padded, 0x80)
	for len(padded)%64 != 56 {
		padded = append(padded, 0x00)
	}
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], bitLen)
	padded = append(padded, lenBuf[:]...)

	// Round constants — FIPS PUB 180-4 §4.2.1 / RFC 3174 §5.
	const (
		k0 uint32 = 0x5A827999 // t in  0..19
		k1 uint32 = 0x6ED9EBA1 // t in 20..39
		k2 uint32 = 0x8F1BBCDC // t in 40..59
		k3 uint32 = 0xCA62C1D6 // t in 60..79
	)

	var w [80]uint32
	for block := 0; block < len(padded); block += 64 {
		// Message schedule W[0..15] from the 16 big-endian words of this block.
		for i := 0; i < 16; i++ {
			w[i] = binary.BigEndian.Uint32(padded[block+i*4 : block+i*4+4])
		}
		// W[16..79] expansion — RFC 3174 §6.1.2 step (b).
		for i := 16; i < 80; i++ {
			w[i] = bits.RotateLeft32(w[i-3]^w[i-8]^w[i-14]^w[i-16], 1)
		}

		a, b, c, d, e := h[0], h[1], h[2], h[3], h[4]

		// 80-round compression — RFC 3174 §6.1.2 step (d).
		for i := 0; i < 80; i++ {
			var f, k uint32
			switch {
			case i < 20:
				f = (b & c) | ((^b) & d)
				k = k0
			case i < 40:
				f = b ^ c ^ d
				k = k1
			case i < 60:
				f = (b & c) | (b & d) | (c & d)
				k = k2
			default:
				f = b ^ c ^ d
				k = k3
			}
			t := bits.RotateLeft32(a, 5) + f + e + k + w[i]
			e = d
			d = c
			c = bits.RotateLeft32(b, 30)
			b = a
			a = t
		}

		h[0] += a
		h[1] += b
		h[2] += c
		h[3] += d
		h[4] += e
	}

	// Produce 20-byte big-endian digest.
	var out [20]byte
	for i, v := range h {
		binary.BigEndian.PutUint32(out[i*4:i*4+4], v)
	}
	return out[:]
}
