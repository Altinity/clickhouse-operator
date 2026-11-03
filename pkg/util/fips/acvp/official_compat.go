//go:build acvp_wrapper

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

package acvp

import (
	"bytes"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha3"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
)

func cmdShakeAFTVOT(newShake func() *sha3.SHAKE) command {
	return command{
		requiredArgs: 2, // message, output length bytes
		handler: func(args [][]byte) ([][]byte, error) {
			msg := args[0]
			outLenBytes := binary.LittleEndian.Uint32(args[1])
			out := make([]byte, outLenBytes)

			h := newShake()
			h.Write(msg)
			h.Read(out)
			return [][]byte{out}, nil
		},
	}
}

func cmdShakeMCT(newShake func() *sha3.SHAKE) command {
	return command{
		requiredArgs: 4, // seed, min output length bytes, max output length bytes, output length bytes
		handler: func(args [][]byte) ([][]byte, error) {
			md := args[0]
			minOutBytes := binary.LittleEndian.Uint32(args[1])
			maxOutBytes := binary.LittleEndian.Uint32(args[2])
			outLenBytes := binary.LittleEndian.Uint32(args[3])
			if outLenBytes < 2 {
				return nil, fmt.Errorf("invalid output length: %d", outLenBytes)
			}

			rangeBytes := maxOutBytes - minOutBytes + 1
			if rangeBytes == 0 {
				return nil, fmt.Errorf("invalid min/max output lengths: %d/%d", minOutBytes, maxOutBytes)
			}

			h := newShake()
			for i := 0; i < 1000; i++ {
				msg := make([]byte, 16)
				copy(msg, md[:minInt(len(md), 16)])

				h.Reset()
				h.Write(msg)
				digest := make([]byte, outLenBytes)
				h.Read(digest)
				md = digest

				rightmost := uint32(md[outLenBytes-2])<<8 | uint32(md[outLenBytes-1])
				outLenBytes = minOutBytes + (rightmost % rangeBytes)
			}

			encodedOutLen := make([]byte, 4)
			binary.LittleEndian.PutUint32(encodedOutLen, outLenBytes)
			return [][]byte{md, encodedOutLen}, nil
		},
	}
}

func cmdCShakeAFT(newCShake func(N, S []byte) *sha3.SHAKE) command {
	return command{
		requiredArgs: 4, // message, output length bytes, function name, customization
		handler: func(args [][]byte) ([][]byte, error) {
			msg := args[0]
			outLenBytes := binary.LittleEndian.Uint32(args[1])
			functionName := args[2]
			customization := args[3]

			out := make([]byte, outLenBytes)
			h := newCShake(functionName, customization)
			h.Write(msg)
			h.Read(out)
			return [][]byte{out}, nil
		},
	}
}

func cmdCShakeMCT(newCShake func(N, S []byte) *sha3.SHAKE) command {
	return command{
		requiredArgs: 6, // msg, min out bytes, max out bytes, out bytes, increment bytes, customization
		handler: func(args [][]byte) ([][]byte, error) {
			msg := args[0]
			minOutLenBytes := binary.LittleEndian.Uint32(args[1])
			maxOutLenBytes := binary.LittleEndian.Uint32(args[2])
			outLenBytes := binary.LittleEndian.Uint32(args[3])
			incrementBytes := binary.LittleEndian.Uint32(args[4])
			customization := args[5]

			if outLenBytes < 2 {
				return nil, fmt.Errorf("invalid output length: %d", outLenBytes)
			}
			rangeBits := (maxOutLenBytes*8 - minOutLenBytes*8) + 1
			if rangeBits == 0 {
				return nil, fmt.Errorf("invalid min/max output lengths: %d/%d", minOutLenBytes, maxOutLenBytes)
			}

			for i := 0; i < 1000; i++ {
				innerMsg := make([]byte, 16)
				copy(innerMsg, msg[:minInt(len(msg), 16)])

				// For MCT, function name is fixed to the empty string.
				h := newCShake(nil, customization)
				h.Write(innerMsg)
				digest := make([]byte, outLenBytes)
				h.Read(digest)
				msg = digest

				rightmostOutputBE := binary.BigEndian.Uint16(digest[outLenBytes-2:])
				incrementBits := incrementBytes * 8
				outLenBits := (minOutLenBytes * 8) + (uint32(rightmostOutputBE)%rangeBits)/incrementBits*incrementBits
				outLenBytes = outLenBits / 8

				msgWithBits := append(innerMsg, digest[len(digest)-2:]...)
				customization = make([]byte, len(msgWithBits))
				for i, b := range msgWithBits {
					customization[i] = (b % 26) + 'A'
				}
			}

			encodedOutLen := make([]byte, 4)
			binary.LittleEndian.PutUint32(encodedOutLen, outLenBytes)
			return [][]byte{msg, encodedOutLen, customization}, nil
		},
	}
}

func cmdHKDFAFT(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 4, // key, salt, info, output length bytes
		handler: func(args [][]byte) ([][]byte, error) {
			key := args[0]
			salt := args[1]
			info := args[2]
			keyLen := int(binary.LittleEndian.Uint32(args[3]))

			out, err := hkdf.Key(hashFn, key, salt, string(info), keyLen)
			if err != nil {
				return nil, err
			}
			return [][]byte{out}, nil
		},
	}
}

func cmdHKDFExtractAFT(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 2, // secret, salt
		handler: func(args [][]byte) ([][]byte, error) {
			secret := args[0]
			salt := args[1]
			out, err := hkdf.Extract(hashFn, secret, salt)
			if err != nil {
				return nil, err
			}
			return [][]byte{out}, nil
		},
	}
}

func cmdHKDFExpandLabelAFT(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 4, // output length bytes, secret, label, transcript hash
		handler: func(args [][]byte) ([][]byte, error) {
			keyLen := int(binary.LittleEndian.Uint32(args[0]))
			secret := args[1]
			label := args[2]
			transcriptHash := args[3]

			out, err := tls13ExpandLabel(hashFn, secret, string(label), transcriptHash, keyLen)
			if err != nil {
				return nil, err
			}
			return [][]byte{out}, nil
		},
	}
}

func tls13ExpandLabel(hashFn func() hash.Hash, secret []byte, label string, context []byte, keyLen int) ([]byte, error) {
	fullLabel := "tls13 " + label
	info := make([]byte, 2+1+len(fullLabel)+1+len(context))
	binary.BigEndian.PutUint16(info[:2], uint16(keyLen))
	info[2] = byte(len(fullLabel))
	copy(info[3:3+len(fullLabel)], fullLabel)
	info[3+len(fullLabel)] = byte(len(context))
	copy(info[4+len(fullLabel):], context)

	return hkdf.Expand(hashFn, secret, string(info), keyLen)
}

func cmdPBKDF() command {
	return command{
		requiredArgs: 5, // hash name, key length bits, salt, password, iteration count
		handler: func(args [][]byte) ([][]byte, error) {
			hashFn, err := lookupHashByName(string(args[0]))
			if err != nil {
				return nil, fmt.Errorf("PBKDF2 failed: %w", err)
			}
			keyLen := int(binary.LittleEndian.Uint32(args[1]) / 8)
			salt := args[2]
			password := args[3]
			iter := int(binary.LittleEndian.Uint32(args[4]))

			derived, err := pbkdf2.Key(hashFn, string(password), salt, iter, keyLen)
			if err != nil {
				return nil, fmt.Errorf("PBKDF2 failed: %w", err)
			}
			return [][]byte{derived}, nil
		},
	}
}

func cmdHMACDRBGAFT(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 6, // output length bytes, entropy, personalization, ad1, ad2, nonce
		handler: func(args [][]byte) ([][]byte, error) {
			outLen := binary.LittleEndian.Uint32(args[0])
			entropy := args[1]
			personalization := args[2]
			ad1 := args[3]
			ad2 := args[4]
			nonce := args[5]

			if len(ad1) != 0 || len(ad2) != 0 {
				return nil, errors.New("additional data not supported")
			}

			out := make([]byte, outLen)
			drbg := newOfficialHMACDRBG(hashFn, entropy, nonce, personalization)
			drbg.generate(out, nil)
			drbg.generate(out, nil)
			return [][]byte{out}, nil
		},
	}
}

type officialHMACDRBG struct {
	hashFn func() hash.Hash
	k      []byte
	v      []byte
}

func newOfficialHMACDRBG(hashFn func() hash.Hash, entropy, nonce, personalization []byte) *officialHMACDRBG {
	size := hashFn().Size()
	k := make([]byte, size)
	v := bytes.Repeat([]byte{1}, size)
	drbg := &officialHMACDRBG{hashFn: hashFn, k: k, v: v}

	seed := make([]byte, 0, len(entropy)+len(nonce)+len(personalization))
	seed = append(seed, entropy...)
	seed = append(seed, nonce...)
	seed = append(seed, personalization...)
	drbg.update(seed)
	return drbg
}

func (d *officialHMACDRBG) update(data []byte) {
	buf := make([]byte, 0, len(d.v)+1+len(data))
	buf = append(buf, d.v...)
	buf = append(buf, 0)
	buf = append(buf, data...)

	mac := hmac.New(d.hashFn, d.k)
	mac.Write(buf)
	d.k = mac.Sum(d.k[:0])

	mac = hmac.New(d.hashFn, d.k)
	mac.Write(d.v)
	d.v = mac.Sum(d.v[:0])

	if len(data) == 0 {
		return
	}

	buf = buf[:0]
	buf = append(buf, d.v...)
	buf = append(buf, 1)
	buf = append(buf, data...)

	mac = hmac.New(d.hashFn, d.k)
	mac.Write(buf)
	d.k = mac.Sum(d.k[:0])

	mac = hmac.New(d.hashFn, d.k)
	mac.Write(d.v)
	d.v = mac.Sum(d.v[:0])
}

func (d *officialHMACDRBG) generate(out []byte, additionalInput []byte) {
	if len(additionalInput) > 0 {
		d.update(additionalInput)
	}

	done := 0
	for done < len(out) {
		mac := hmac.New(d.hashFn, d.k)
		mac.Write(d.v)
		d.v = mac.Sum(d.v[:0])
		done += copy(out[done:], d.v)
	}

	d.update(additionalInput)
}

func lookupHashByName(name string) (func() hash.Hash, error) {
	switch name {
	case "SHA-1":
		return sha1.New, nil
	case "SHA2-224":
		return sha256.New224, nil
	case "SHA2-256":
		return sha256.New, nil
	case "SHA2-384":
		return sha512.New384, nil
	case "SHA2-512":
		return sha512.New, nil
	case "SHA2-512/224", "SHA2-512-224":
		return sha512.New512_224, nil
	case "SHA2-512/256", "SHA2-512-256":
		return sha512.New512_256, nil
	case "SHA3-224":
		return newSHA3224, nil
	case "SHA3-256":
		return newSHA3256, nil
	case "SHA3-384":
		return newSHA3384, nil
	case "SHA3-512":
		return newSHA3512, nil
	default:
		return nil, fmt.Errorf("unknown hash name: %q", name)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
