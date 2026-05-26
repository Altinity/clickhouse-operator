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
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/mlkem"
	"crypto/mlkem/mlkemtest"
	cryptoRand "crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/big"
	"strings"
	"sync"
)

func cmdMLDSA44KeyGen() command { return cmdUnsupportedMLDSA(1) }
func cmdMLDSA65KeyGen() command { return cmdUnsupportedMLDSA(1) }
func cmdMLDSA87KeyGen() command { return cmdUnsupportedMLDSA(1) }
func cmdMLDSA44SigGen() command { return cmdUnsupportedMLDSA(5) }
func cmdMLDSA65SigGen() command { return cmdUnsupportedMLDSA(5) }
func cmdMLDSA87SigGen() command { return cmdUnsupportedMLDSA(5) }
func cmdMLDSA44SigVer() command { return cmdUnsupportedMLDSA(5) }
func cmdMLDSA65SigVer() command { return cmdUnsupportedMLDSA(5) }
func cmdMLDSA87SigVer() command { return cmdUnsupportedMLDSA(5) }

func cmdUnsupportedMLDSA(requiredArgs int) command {
	return command{
		requiredArgs: requiredArgs,
		handler: func(args [][]byte) ([][]byte, error) {
			return nil, errors.New("ML-DSA is not implemented with public Go crypto APIs in this build")
		},
	}
}

func cmdEDDSAKeyGen() command {
	return command{
		requiredArgs: 1, // curve
		handler: func(args [][]byte) ([][]byte, error) {
			if string(args[0]) != "ED-25519" {
				return nil, fmt.Errorf("unsupported EDDSA curve: %q", args[0])
			}
			pub, priv, err := ed25519.GenerateKey(cryptoRand.Reader)
			if err != nil {
				return nil, err
			}
			return [][]byte{priv.Seed(), pub}, nil
		},
	}
}

func cmdEDDSAKeyVer() command {
	return command{
		requiredArgs: 2, // curve, public key
		handler: func(args [][]byte) ([][]byte, error) {
			if string(args[0]) != "ED-25519" {
				return nil, fmt.Errorf("unsupported EDDSA curve: %q", args[0])
			}
			if len(args[1]) != ed25519.PublicKeySize {
				return [][]byte{{0}}, nil
			}
			// ACVP keyVer requires curve-point validation independent of signature checks.
			err := ed25519.VerifyWithOptions(ed25519.PublicKey(args[1]), []byte{0}, make([]byte, ed25519.SignatureSize), &ed25519.Options{})
			if err != nil && err.Error() == "ed25519: bad public key" {
				return [][]byte{{0}}, nil
			}
			return [][]byte{{1}}, nil
		},
	}
}

func cmdEDDSASigGen() command {
	return command{
		requiredArgs: 5, // curve, private key seed, message, prehash, context
		handler: func(args [][]byte) ([][]byte, error) {
			if string(args[0]) != "ED-25519" {
				return nil, fmt.Errorf("unsupported EDDSA curve: %q", args[0])
			}
			if len(args[1]) != ed25519.SeedSize {
				return nil, fmt.Errorf("invalid EDDSA seed size: %d", len(args[1]))
			}
			priv := ed25519.NewKeyFromSeed(args[1])
			msg := args[2]
			prehash := len(args[3]) > 0 && args[3][0] == 1
			context := string(args[4])

			if prehash {
				sum := sha512.Sum512(msg)
				sig, err := priv.Sign(cryptoRand.Reader, sum[:], &ed25519.Options{
					Hash:    crypto.SHA512,
					Context: context,
				})
				if err != nil {
					return nil, err
				}
				return [][]byte{sig}, nil
			}
			return [][]byte{ed25519.Sign(priv, msg)}, nil
		},
	}
}

func cmdEDDSASigVer() command {
	return command{
		requiredArgs: 5, // curve, message, public key, signature, prehash
		handler: func(args [][]byte) ([][]byte, error) {
			if string(args[0]) != "ED-25519" {
				return nil, fmt.Errorf("unsupported EDDSA curve: %q", args[0])
			}
			pub := ed25519.PublicKey(args[2])
			msg := args[1]
			sig := args[3]
			prehash := len(args[4]) > 0 && args[4][0] == 1

			if prehash {
				sum := sha512.Sum512(msg)
				err := ed25519.VerifyWithOptions(pub, sum[:], sig, &ed25519.Options{Hash: crypto.SHA512})
				if err != nil {
					return [][]byte{{0}}, nil
				}
				return [][]byte{{1}}, nil
			}
			if ed25519.Verify(pub, msg, sig) {
				return [][]byte{{1}}, nil
			}
			return [][]byte{{0}}, nil
		},
	}
}

func cmdECDSAKeyGen() command {
	return command{
		requiredArgs: 1, // curve
		handler: func(args [][]byte) ([][]byte, error) {
			curve, err := lookupCurveByName(string(args[0]))
			if err != nil {
				return nil, err
			}
			priv, err := ecdsa.GenerateKey(curve, cryptoRand.Reader)
			if err != nil {
				return nil, err
			}
			pubBytes := elliptic.Marshal(curve, priv.X, priv.Y)
			byteLen := (curve.Params().BitSize + 7) / 8
			return [][]byte{
				priv.D.Bytes(),
				pubBytes[1 : 1+byteLen],
				pubBytes[1+byteLen:],
			}, nil
		},
	}
}

func cmdECDSAKeyVer() command {
	return command{
		requiredArgs: 3, // curve, x, y
		handler: func(args [][]byte) ([][]byte, error) {
			curve, err := lookupCurveByName(string(args[0]))
			if err != nil {
				return nil, err
			}
			x := new(big.Int).SetBytes(args[1])
			y := new(big.Int).SetBytes(args[2])
			if curve.IsOnCurve(x, y) {
				return [][]byte{{1}}, nil
			}
			return [][]byte{{0}}, nil
		},
	}
}

type deterministicReader struct {
	seed []byte
	ctr  uint64
	buf  []byte
}

func (d *deterministicReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		if len(d.buf) == 0 {
			h := sha512.New()
			h.Write(d.seed)
			var ctr [8]byte
			binary.BigEndian.PutUint64(ctr[:], d.ctr)
			d.ctr++
			h.Write(ctr[:])
			d.buf = h.Sum(nil)
		}
		c := copy(p[n:], d.buf)
		d.buf = d.buf[c:]
		n += c
	}
	return n, nil
}

func cmdECDSASigGen(deterministic bool) command {
	return command{
		requiredArgs: 4, // curve, private key, hash name, message
		handler: func(args [][]byte) ([][]byte, error) {
			curve, err := lookupCurveByName(string(args[0]))
			if err != nil {
				return nil, err
			}
			hashFn, err := lookupHashByName(string(args[2]))
			if err != nil {
				return nil, err
			}
			d := new(big.Int).SetBytes(args[1])
			x, y := curve.ScalarBaseMult(d.Bytes())
			priv := &ecdsa.PrivateKey{
				PublicKey: ecdsa.PublicKey{Curve: curve, X: x, Y: y},
				D:         d,
			}

			h := hashFn()
			h.Write(args[3])
			digest := h.Sum(nil)

			reader := io.Reader(cryptoRand.Reader)
			if deterministic {
				seed := make([]byte, 0, len(args[1])+len(args[3]))
				seed = append(seed, args[1]...)
				seed = append(seed, args[3]...)
				reader = &deterministicReader{seed: seed}
			}
			r, s, err := ecdsa.Sign(reader, priv, digest)
			if err != nil {
				return nil, err
			}
			return [][]byte{r.Bytes(), s.Bytes()}, nil
		},
	}
}

func cmdECDSASigVer() command {
	return command{
		requiredArgs: 7, // curve, hash, message, x, y, r, s
		handler: func(args [][]byte) ([][]byte, error) {
			curve, err := lookupCurveByName(string(args[0]))
			if err != nil {
				return nil, err
			}
			hashFn, err := lookupHashByName(string(args[1]))
			if err != nil {
				return nil, err
			}
			pub := &ecdsa.PublicKey{
				Curve: curve,
				X:     new(big.Int).SetBytes(args[3]),
				Y:     new(big.Int).SetBytes(args[4]),
			}
			h := hashFn()
			h.Write(args[2])
			digest := h.Sum(nil)
			if ecdsa.Verify(pub, digest, new(big.Int).SetBytes(args[5]), new(big.Int).SetBytes(args[6])) {
				return [][]byte{{1}}, nil
			}
			return [][]byte{{0}}, nil
		},
	}
}

func lookupCurveByName(name string) (elliptic.Curve, error) {
	switch name {
	case "P-224":
		return elliptic.P224(), nil
	case "P-256":
		return elliptic.P256(), nil
	case "P-384":
		return elliptic.P384(), nil
	case "P-521":
		return elliptic.P521(), nil
	default:
		return nil, fmt.Errorf("unknown curve name: %q", name)
	}
}

func cmdCMACAESAFT() command {
	return command{
		requiredArgs: 3, // output bytes, key, message
		handler: func(args [][]byte) ([][]byte, error) {
			outLen := int(binary.LittleEndian.Uint32(args[0]))
			tag, err := aesCMAC(args[1], args[2])
			if err != nil {
				return nil, err
			}
			if outLen > len(tag) {
				return nil, fmt.Errorf("invalid output length: %d", outLen)
			}
			return [][]byte{tag[:outLen]}, nil
		},
	}
}

func cmdCMACAESVerifyAFT() command {
	return command{
		requiredArgs: 3, // key, message, claimed tag
		handler: func(args [][]byte) ([][]byte, error) {
			tag, err := aesCMAC(args[0], args[1])
			if err != nil {
				return nil, err
			}
			claimed := args[2]
			if len(claimed) > len(tag) || subtle.ConstantTimeCompare(tag[:len(claimed)], claimed) != 1 {
				return [][]byte{{0}}, nil
			}
			return [][]byte{{1}}, nil
		},
	}
}

func aesCMAC(key, msg []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	L := make([]byte, aes.BlockSize)
	block.Encrypt(L, make([]byte, aes.BlockSize))
	k1 := leftShiftOneBit(L)
	if L[0]&0x80 != 0 {
		k1[len(k1)-1] ^= 0x87
	}
	k2 := leftShiftOneBit(k1)
	if k1[0]&0x80 != 0 {
		k2[len(k2)-1] ^= 0x87
	}

	n := (len(msg) + aes.BlockSize - 1) / aes.BlockSize
	if n == 0 {
		n = 1
	}

	last := make([]byte, aes.BlockSize)
	complete := len(msg) > 0 && len(msg)%aes.BlockSize == 0
	if complete {
		copy(last, msg[(n-1)*aes.BlockSize:])
		xorBlock(last, k1)
	} else {
		start := (n - 1) * aes.BlockSize
		if start < len(msg) {
			copy(last, msg[start:])
		}
		last[len(msg)-start] = 0x80
		xorBlock(last, k2)
	}

	x := make([]byte, aes.BlockSize)
	y := make([]byte, aes.BlockSize)
	for i := 0; i < n-1; i++ {
		copy(y, msg[i*aes.BlockSize:(i+1)*aes.BlockSize])
		xorBlock(y, x)
		block.Encrypt(x, y)
	}
	copy(y, last)
	xorBlock(y, x)
	block.Encrypt(x, y)
	return x, nil
}

func leftShiftOneBit(in []byte) []byte {
	out := make([]byte, len(in))
	var carry byte
	for i := len(in) - 1; i >= 0; i-- {
		out[i] = (in[i] << 1) | carry
		carry = (in[i] >> 7) & 1
	}
	return out
}

func xorBlock(dst, src []byte) {
	for i := range dst {
		dst[i] ^= src[i]
	}
}

func cmdKDFCounter() command {
	return command{
		requiredArgs: 5, // output bytes, prf, counter location, key, counter bits
		handler: func(args [][]byte) ([][]byte, error) {
			outputBytes := int(binary.LittleEndian.Uint32(args[0]))
			prf := string(args[1])
			counterLocation := string(args[2])
			key := args[3]
			counterBits := binary.LittleEndian.Uint32(args[4])

			if outputBytes <= 0 || outputBytes > 4096 {
				return nil, fmt.Errorf("unsupported output length %d", outputBytes)
			}
			if prf != "CMAC-AES128" && prf != "CMAC-AES192" && prf != "CMAC-AES256" {
				return nil, fmt.Errorf("unsupported PRF %q", prf)
			}
			if counterLocation != "before fixed data" {
				return nil, fmt.Errorf("unsupported counter location %q", counterLocation)
			}
			if len(key) == 0 {
				return nil, errors.New("deferred test cases are not supported")
			}
			if counterBits != 16 {
				return nil, fmt.Errorf("unsupported counter bits %d", counterBits)
			}

			var context [12]byte
			if _, err := cryptoRand.Read(context[:]); err != nil {
				return nil, err
			}
			fixedData := make([]byte, 14) // label(1) + separator(1) + context(12)
			copy(fixedData[2:], context[:])

			result := make([]byte, 0, outputBytes)
			for i := 1; len(result) < outputBytes; i++ {
				input := make([]byte, 2+len(fixedData))
				binary.BigEndian.PutUint16(input[:2], uint16(i))
				copy(input[2:], fixedData)
				tag, err := aesCMAC(key, input)
				if err != nil {
					return nil, err
				}
				result = append(result, tag...)
			}
			return [][]byte{key, fixedData, result[:outputBytes]}, nil
		},
	}
}

func cmdKDFFeedback() command {
	return command{
		requiredArgs: 5, // output bytes, prf, counter location, key, counter bits
		handler: func(args [][]byte) ([][]byte, error) {
			outputBytes := int(binary.LittleEndian.Uint32(args[0]))
			prf := string(args[1])
			counterLocation := string(args[2])
			key := args[3]
			counterBits := binary.LittleEndian.Uint32(args[4])

			if !strings.HasPrefix(prf, "HMAC-") {
				return nil, fmt.Errorf("unsupported PRF %q", prf)
			}
			hashName := prf[len("HMAC-"):]
			hashFn, err := lookupHashByName(hashName)
			if err != nil {
				return nil, err
			}
			if counterLocation != "after fixed data" {
				return nil, fmt.Errorf("unsupported counter location %q", counterLocation)
			}
			if len(key) == 0 {
				return nil, errors.New("deferred test cases are not supported")
			}
			if counterBits != 8 {
				return nil, fmt.Errorf("unsupported counter bits %d", counterBits)
			}

			var context [12]byte
			if _, err := cryptoRand.Read(context[:]); err != nil {
				return nil, err
			}
			fixedData := make([]byte, 14) // label(1) + separator(1) + context(12)
			copy(fixedData[2:], context[:])

			out, err := hkdf.Expand(hashFn, key, string(fixedData), outputBytes)
			if err != nil {
				return nil, err
			}
			return [][]byte{key, fixedData, out}, nil
		},
	}
}

func cmdOneStepNoCounterHMAC(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 4, // key, info, salt, output bytes
		handler: func(args [][]byte) ([][]byte, error) {
			key := args[0]
			info := args[1]
			salt := args[2]
			outBytes := int(binary.LittleEndian.Uint32(args[3]))

			mac := hmac.New(hashFn, salt)
			if outBytes != mac.Size() {
				return nil, fmt.Errorf("invalid output length: got %d want %d", outBytes, mac.Size())
			}
			data := make([]byte, 0, len(key)+len(info))
			data = append(data, key...)
			data = append(data, info...)
			mac.Write(data)
			return [][]byte{mac.Sum(nil)}, nil
		},
	}
}

var (
	rsaKeyCache   = map[int]*rsa.PrivateKey{}
	rsaKeyCacheMu sync.Mutex
)

func getRSAKey(bits int) (*rsa.PrivateKey, error) {
	rsaKeyCacheMu.Lock()
	defer rsaKeyCacheMu.Unlock()
	if k, ok := rsaKeyCache[bits]; ok {
		return k, nil
	}
	k, err := rsa.GenerateKey(cryptoRand.Reader, bits)
	if err != nil {
		return nil, err
	}
	rsaKeyCache[bits] = k
	return k, nil
}

func cmdRSAKeyGen() command {
	return command{
		requiredArgs: 1, // modulus bits
		handler: func(args [][]byte) ([][]byte, error) {
			bits := int(binary.LittleEndian.Uint32(args[0]))
			key, err := getRSAKey(bits)
			if err != nil {
				return nil, err
			}
			eBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(eBytes, uint32(key.E))
			return [][]byte{eBytes, key.Primes[0].Bytes(), key.Primes[1].Bytes(), key.N.Bytes(), key.D.Bytes()}, nil
		},
	}
}

func cmdRSASigGen(hashFn func() hash.Hash, hashID crypto.Hash, pss bool) command {
	return command{
		requiredArgs: 2, // modulus bits, message
		handler: func(args [][]byte) ([][]byte, error) {
			bits := int(binary.LittleEndian.Uint32(args[0]))
			msg := args[1]
			key, err := getRSAKey(bits)
			if err != nil {
				return nil, err
			}
			h := hashFn()
			h.Write(msg)
			digest := h.Sum(nil)

			var sig []byte
			if !pss {
				sig, err = rsa.SignPKCS1v15(cryptoRand.Reader, key, hashID, digest)
			} else {
				sig, err = rsa.SignPSS(cryptoRand.Reader, key, hashID, digest, &rsa.PSSOptions{
					SaltLength: h.Size(),
					Hash:       hashID,
				})
			}
			if err != nil {
				return nil, err
			}

			eBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(eBytes, uint32(key.E))
			return [][]byte{key.N.Bytes(), eBytes, sig}, nil
		},
	}
}

func cmdRSASigVer(hashFn func() hash.Hash, hashID crypto.Hash, pss bool) command {
	return command{
		requiredArgs: 4, // n, e, message, signature
		handler: func(args [][]byte) ([][]byte, error) {
			n := new(big.Int).SetBytes(args[0])
			ePadded := make([]byte, 4)
			copy(ePadded[4-len(args[1]):], args[1])
			e := int(binary.BigEndian.Uint32(ePadded))
			pub := &rsa.PublicKey{N: n, E: e}

			h := hashFn()
			h.Write(args[2])
			digest := h.Sum(nil)
			var err error
			if !pss {
				err = rsa.VerifyPKCS1v15(pub, hashID, digest, args[3])
			} else {
				err = rsa.VerifyPSS(pub, hashID, digest, args[3], nil)
			}
			if err != nil {
				return [][]byte{{0}}, nil
			}
			return [][]byte{{1}}, nil
		},
	}
}

func cmdRSASigGenByHashName(hashName string, pss bool) command {
	hashFn, err := lookupHashByName(hashName)
	if err != nil {
		panic(err)
	}
	hashID, err := hashNameToCryptoHash(hashName)
	if err != nil {
		panic(err)
	}
	return cmdRSASigGen(hashFn, hashID, pss)
}

func cmdRSASigVerByHashName(hashName string, pss bool) command {
	hashFn, err := lookupHashByName(hashName)
	if err != nil {
		panic(err)
	}
	hashID, err := hashNameToCryptoHash(hashName)
	if err != nil {
		panic(err)
	}
	return cmdRSASigVer(hashFn, hashID, pss)
}

func hashNameToCryptoHash(name string) (crypto.Hash, error) {
	switch name {
	case "SHA-1":
		return crypto.SHA1, nil
	case "SHA2-224":
		return crypto.SHA224, nil
	case "SHA2-256":
		return crypto.SHA256, nil
	case "SHA2-384":
		return crypto.SHA384, nil
	case "SHA2-512":
		return crypto.SHA512, nil
	default:
		return 0, fmt.Errorf("unsupported hash for RSA: %s", name)
	}
}

func cmdTLSKDF12(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 5, // output bytes, secret, label, seed1, seed2
		handler: func(args [][]byte) ([][]byte, error) {
			outLen := int(binary.LittleEndian.Uint32(args[0]))
			secret := args[1]
			label := args[2]
			seed := append(append([]byte{}, label...), args[3]...)
			seed = append(seed, args[4]...)
			return [][]byte{tls12PRF(hashFn, secret, seed, outLen)}, nil
		},
	}
}

func tls12PRF(hashFn func() hash.Hash, secret, seed []byte, outLen int) []byte {
	out := make([]byte, 0, outLen)
	a := hmacDigest(hashFn, secret, seed)
	for len(out) < outLen {
		block := hmacDigest(hashFn, secret, append(append([]byte{}, a...), seed...))
		out = append(out, block...)
		a = hmacDigest(hashFn, secret, a)
	}
	return out[:outLen]
}

func hmacDigest(hashFn func() hash.Hash, key, data []byte) []byte {
	m := hmac.New(hashFn, key)
	m.Write(data)
	return m.Sum(nil)
}

func cmdSSHKDF(hashFn func() hash.Hash, client bool) command {
	return command{
		requiredArgs: 4, // K, H, session id, cipher
		handler: func(args [][]byte) ([][]byte, error) {
			k := args[0]
			h := args[1]
			sessionID := args[2]
			cipherName := string(args[3])

			var keyLen int
			switch cipherName {
			case "AES-128":
				keyLen = 16
			case "AES-192":
				keyLen = 24
			case "AES-256":
				keyLen = 32
			default:
				return nil, fmt.Errorf("unsupported cipher: %q", cipherName)
			}

			ivLabel, encLabel, intLabel := byte('A'), byte('C'), byte('E')
			if !client {
				ivLabel, encLabel, intLabel = 'B', 'D', 'F'
			}
			ivKey := sshDerive(hashFn, k, h, sessionID, ivLabel, 16)
			encKey := sshDerive(hashFn, k, h, sessionID, encLabel, keyLen)
			intKey := sshDerive(hashFn, k, h, sessionID, intLabel, hashFn().Size())
			return [][]byte{ivKey, encKey, intKey}, nil
		},
	}
}

func sshDerive(hashFn func() hash.Hash, k, h, sessionID []byte, letter byte, outLen int) []byte {
	out := make([]byte, 0, outLen)
	for len(out) < outLen {
		hasher := hashFn()
		hasher.Write(k)
		hasher.Write(h)
		if len(out) == 0 {
			hasher.Write([]byte{letter})
			hasher.Write(sessionID)
		} else {
			hasher.Write(out)
		}
		out = append(out, hasher.Sum(nil)...)
	}
	return out[:outLen]
}

func cmdECDH(curve elliptic.Curve) command {
	return command{
		requiredArgs: 3, // peer x, peer y, private key (optional)
		handler: func(args [][]byte) ([][]byte, error) {
			peerX := new(big.Int).SetBytes(args[0])
			peerY := new(big.Int).SetBytes(args[1])
			if !curve.IsOnCurve(peerX, peerY) {
				return nil, errors.New("invalid peer point")
			}

			byteLen := (curve.Params().BitSize + 7) / 8
			sk := append([]byte(nil), args[2]...)
			if len(sk) == 0 {
				sk = make([]byte, byteLen)
				for {
					if _, err := cryptoRand.Read(sk); err != nil {
						return nil, err
					}
					s := new(big.Int).SetBytes(sk)
					if s.Sign() > 0 && s.Cmp(curve.Params().N) < 0 {
						break
					}
				}
			}

			pubX, pubY := curve.ScalarBaseMult(sk)
			secretX, _ := curve.ScalarMult(peerX, peerY, sk)
			if secretX == nil {
				return nil, errors.New("ecdh failed")
			}
			return [][]byte{
				pubX.FillBytes(make([]byte, byteLen)),
				pubY.FillBytes(make([]byte, byteLen)),
				secretX.FillBytes(make([]byte, byteLen)),
			}, nil
		},
	}
}

type acvpCTRDRBG struct {
	block cipher.Block
	v     [aes.BlockSize]byte
}

func newACVPCTRDRBG(entropy *[48]byte) (*acvpCTRDRBG, error) {
	key := make([]byte, 32)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	d := &acvpCTRDRBG{block: block}
	d.v[len(d.v)-1] = 1
	if err := d.update(entropy); err != nil {
		return nil, err
	}
	return d, nil
}

func increment128(v *[16]byte) {
	for i := len(v) - 1; i >= 0; i-- {
		v[i]++
		if v[i] != 0 {
			return
		}
	}
}

func (d *acvpCTRDRBG) generateKeystream(out []byte) {
	blockBuf := make([]byte, aes.BlockSize)
	done := 0
	for done < len(out) {
		d.block.Encrypt(blockBuf, d.v[:])
		increment128(&d.v)
		done += copy(out[done:], blockBuf)
	}
}

func (d *acvpCTRDRBG) update(seed *[48]byte) error {
	temp := make([]byte, 48)
	d.generateKeystream(temp)
	for i := range temp {
		temp[i] ^= seed[i]
	}

	block, err := aes.NewCipher(temp[:32])
	if err != nil {
		return err
	}
	d.block = block
	copy(d.v[:], temp[32:])
	increment128(&d.v)
	return nil
}

func (d *acvpCTRDRBG) generate(out []byte, additionalInput *[48]byte) error {
	if additionalInput != nil {
		if err := d.update(additionalInput); err != nil {
			return err
		}
	} else {
		additionalInput = new([48]byte)
	}

	d.generateKeystream(out)
	if rem := len(out) % aes.BlockSize; rem != 0 {
		d.generateKeystream(make([]byte, aes.BlockSize-rem))
	}
	return d.update(additionalInput)
}

func require48Bytes(input []byte) (*[48]byte, error) {
	if len(input) != 48 {
		return nil, fmt.Errorf("invalid length: %d", len(input))
	}
	return (*[48]byte)(input), nil
}

func cmdCTRDRBGAFT() command {
	return command{
		requiredArgs: 6, // outLen, entropy, personalization, ad1, ad2, nonce
		handler: func(args [][]byte) ([][]byte, error) {
			outLen := binary.LittleEndian.Uint32(args[0])
			entropy, err := require48Bytes(args[1])
			if err != nil {
				return nil, fmt.Errorf("entropy: %w", err)
			}
			if len(args[2]) > 0 {
				return nil, errors.New("personalization string not supported")
			}
			ad1, err := require48Bytes(args[3])
			if err != nil {
				return nil, fmt.Errorf("ad1: %w", err)
			}
			ad2, err := require48Bytes(args[4])
			if err != nil {
				return nil, fmt.Errorf("ad2: %w", err)
			}
			if len(args[5]) > 0 {
				return nil, errors.New("unexpected nonce value")
			}

			drbg, err := newACVPCTRDRBG(entropy)
			if err != nil {
				return nil, err
			}
			out := make([]byte, outLen)
			if err := drbg.generate(out, ad1); err != nil {
				return nil, err
			}
			if err := drbg.generate(out, ad2); err != nil {
				return nil, err
			}
			return [][]byte{out}, nil
		},
	}
}

func cmdCTRDRBGReseedAFT() command {
	return command{
		requiredArgs: 8, // outLen, entropy, personalization, reseedAD, reseedEntropy, ad1, ad2, nonce
		handler: func(args [][]byte) ([][]byte, error) {
			outLen := binary.LittleEndian.Uint32(args[0])
			entropy, err := require48Bytes(args[1])
			if err != nil {
				return nil, fmt.Errorf("entropy: %w", err)
			}
			if len(args[2]) > 0 {
				return nil, errors.New("personalization string not supported")
			}
			reseedAD, err := require48Bytes(args[3])
			if err != nil {
				return nil, fmt.Errorf("reseed ad: %w", err)
			}
			reseedEntropy, err := require48Bytes(args[4])
			if err != nil {
				return nil, fmt.Errorf("reseed entropy: %w", err)
			}
			ad1, err := require48Bytes(args[5])
			if err != nil {
				return nil, fmt.Errorf("ad1: %w", err)
			}
			ad2, err := require48Bytes(args[6])
			if err != nil {
				return nil, fmt.Errorf("ad2: %w", err)
			}
			if len(args[7]) > 0 {
				return nil, errors.New("unexpected nonce value")
			}

			drbg, err := newACVPCTRDRBG(entropy)
			if err != nil {
				return nil, err
			}
			var reseedSeed [48]byte
			for i := range reseedSeed {
				reseedSeed[i] = reseedEntropy[i] ^ reseedAD[i]
			}
			if err := drbg.update(&reseedSeed); err != nil {
				return nil, err
			}

			out := make([]byte, outLen)
			if err := drbg.generate(out, ad1); err != nil {
				return nil, err
			}
			if err := drbg.generate(out, ad2); err != nil {
				return nil, err
			}
			return [][]byte{out}, nil
		},
	}
}

func cmdMLKEM768KeyGen() command {
	return command{
		requiredArgs: 1, // seed (d||z)
		handler: func(args [][]byte) ([][]byte, error) {
			dk, err := mlkem.NewDecapsulationKey768(args[0])
			if err != nil {
				return nil, err
			}
			return [][]byte{dk.EncapsulationKey().Bytes(), dk.Bytes()}, nil
		},
	}
}

func cmdMLKEM768Encap() command {
	return command{
		requiredArgs: 2, // encapsulation key, entropy
		handler: func(args [][]byte) ([][]byte, error) {
			ek, err := mlkem.NewEncapsulationKey768(args[0])
			if err != nil {
				return nil, err
			}
			ss, ct, err := mlkemtest.Encapsulate768(ek, args[1])
			if err != nil {
				return nil, err
			}
			return [][]byte{ct, ss}, nil
		},
	}
}

func cmdMLKEM768Decap() command {
	return command{
		requiredArgs: 2, // decapsulation key seed, ciphertext
		handler: func(args [][]byte) ([][]byte, error) {
			seed := args[0]
			if len(seed) != mlkem.SeedSize && len(seed) > mlkem.SeedSize {
				seed = seed[:mlkem.SeedSize]
			}
			dk, err := mlkem.NewDecapsulationKey768(seed)
			if err != nil {
				return nil, err
			}
			ss, err := dk.Decapsulate(args[1])
			if err != nil {
				return nil, err
			}
			return [][]byte{ss}, nil
		},
	}
}

func cmdMLKEM1024KeyGen() command {
	return command{
		requiredArgs: 1, // seed (d||z)
		handler: func(args [][]byte) ([][]byte, error) {
			dk, err := mlkem.NewDecapsulationKey1024(args[0])
			if err != nil {
				return nil, err
			}
			return [][]byte{dk.EncapsulationKey().Bytes(), dk.Bytes()}, nil
		},
	}
}

func cmdMLKEM1024Encap() command {
	return command{
		requiredArgs: 2, // encapsulation key, entropy
		handler: func(args [][]byte) ([][]byte, error) {
			ek, err := mlkem.NewEncapsulationKey1024(args[0])
			if err != nil {
				return nil, err
			}
			ss, ct, err := mlkemtest.Encapsulate1024(ek, args[1])
			if err != nil {
				return nil, err
			}
			return [][]byte{ct, ss}, nil
		},
	}
}

func cmdMLKEM1024Decap() command {
	return command{
		requiredArgs: 2, // decapsulation key seed, ciphertext
		handler: func(args [][]byte) ([][]byte, error) {
			seed := args[0]
			if len(seed) != mlkem.SeedSize && len(seed) > mlkem.SeedSize {
				seed = seed[:mlkem.SeedSize]
			}
			dk, err := mlkem.NewDecapsulationKey1024(seed)
			if err != nil {
				return nil, err
			}
			ss, err := dk.Decapsulate(args[1])
			if err != nil {
				return nil, err
			}
			return [][]byte{ss}, nil
		},
	}
}

func (d *officialHMACDRBG) reseed(entropy, additionalInput []byte) {
	seed := make([]byte, 0, len(entropy)+len(additionalInput))
	seed = append(seed, entropy...)
	seed = append(seed, additionalInput...)
	d.update(seed)
}

func cmdHMACDRBGReseed(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 8, // outLen, entropy, personalization, reseedAD, reseedEntropy, ad1, ad2, nonce
		handler: func(args [][]byte) ([][]byte, error) {
			outLen := binary.LittleEndian.Uint32(args[0])
			out := make([]byte, outLen)
			drbg := newOfficialHMACDRBG(hashFn, args[1], args[7], args[2])
			drbg.reseed(args[4], args[3])
			drbg.generate(out, args[5])
			drbg.generate(out, args[6])
			return [][]byte{out}, nil
		},
	}
}

func cmdHMACDRBGPredictionResistance(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 8, // outLen, entropy, personalization, ad1, entropy1, ad2, entropy2, nonce
		handler: func(args [][]byte) ([][]byte, error) {
			outLen := binary.LittleEndian.Uint32(args[0])
			out := make([]byte, outLen)
			drbg := newOfficialHMACDRBG(hashFn, args[1], args[7], args[2])
			drbg.reseed(args[4], args[3])
			drbg.generate(out, nil)
			drbg.reseed(args[6], args[5])
			drbg.generate(out, nil)
			return [][]byte{out}, nil
		},
	}
}
