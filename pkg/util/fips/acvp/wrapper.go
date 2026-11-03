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
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/elliptic"
	"crypto/fips140"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha3"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
)

type request struct {
	name string
	args [][]byte
}

type commandHandler func([][]byte) ([][]byte, error)

type command struct {
	requiredArgs int
	handler      commandHandler
}

var capabilitiesJSON = []byte(`[
  {
    "algorithm": "SHA-1",
    "revision": "1.0",
    "messageLength": [{ "min": 0, "max": 65528, "increment": 8 }],
    "performLargeDataTest": [1, 2, 4, 8]
  },
  {
    "algorithm": "SHA2-224",
    "revision": "1.0",
    "messageLength": [{ "min": 0, "max": 65528, "increment": 8 }],
    "performLargeDataTest": [1, 2, 4, 8]
  },
  {
    "algorithm": "SHA2-256",
    "revision": "1.0",
    "messageLength": [{ "min": 0, "max": 65528, "increment": 8 }],
    "performLargeDataTest": [1, 2, 4, 8]
  },
  {
    "algorithm": "SHA2-384",
    "revision": "1.0",
    "messageLength": [{ "min": 0, "max": 65528, "increment": 8 }],
    "performLargeDataTest": [1, 2, 4, 8]
  },
  {
    "algorithm": "SHA2-512",
    "revision": "1.0",
    "messageLength": [{ "min": 0, "max": 65528, "increment": 8 }],
    "performLargeDataTest": [1, 2, 4, 8]
  },
  {
    "algorithm": "SHA2-512/224",
    "revision": "1.0",
    "messageLength": [{ "min": 0, "max": 65528, "increment": 8 }],
    "performLargeDataTest": [1, 2, 4, 8]
  },
  {
    "algorithm": "SHA2-512/256",
    "revision": "1.0",
    "messageLength": [{ "min": 0, "max": 65528, "increment": 8 }],
    "performLargeDataTest": [1, 2, 4, 8]
  },
  {
    "algorithm": "SHA3-224",
    "revision": "2.0",
    "messageLength": [{ "min": 0, "max": 65536, "increment": 8 }],
    "performLargeDataTest": [1, 2, 4, 8]
  },
  {
    "algorithm": "SHA3-256",
    "revision": "2.0",
    "messageLength": [{ "min": 0, "max": 65536, "increment": 8 }],
    "performLargeDataTest": [1, 2, 4, 8]
  },
  {
    "algorithm": "SHA3-384",
    "revision": "2.0",
    "messageLength": [{ "min": 0, "max": 65536, "increment": 8 }],
    "performLargeDataTest": [1, 2, 4, 8]
  },
  {
    "algorithm": "SHA3-512",
    "revision": "2.0",
    "messageLength": [{ "min": 0, "max": 65536, "increment": 8 }],
    "performLargeDataTest": [1, 2, 4, 8]
  },
  {
    "algorithm": "ACVP-AES-ECB",
    "revision": "1.0",
    "direction": ["encrypt", "decrypt"],
    "keyLen": [128, 192, 256]
  },
  {
    "algorithm": "ACVP-AES-CTR",
    "revision": "1.0",
    "direction": ["encrypt", "decrypt"],
    "keyLen": [128, 192, 256],
    "payloadLen": [{ "min": 8, "max": 128, "increment": 8 }],
    "incrementalCounter": true,
    "overflowCounter": true,
    "performCounterTests": true
  },
  {
    "algorithm": "ACVP-AES-CBC",
    "revision": "1.0",
    "direction": ["encrypt", "decrypt"],
    "keyLen": [128, 192, 256]
  },
  {
    "algorithm": "ACVP-AES-GCM",
    "revision": "1.0",
    "direction": ["encrypt", "decrypt"],
    "keyLen": [128, 192, 256],
    "payloadLen": [{ "min": 0, "max": 65536, "increment": 8 }],
    "aadLen": [{ "min": 0, "max": 65536, "increment": 8 }],
    "tagLen": [32, 64, 96, 104, 112, 120, 128],
    "ivLen": [96],
    "ivGen": ["external"]
  },
  {
    "algorithm": "ACVP-AES-GMAC",
    "revision": "1.0",
    "direction": ["encrypt", "decrypt"],
    "keyLen": [128, 192, 256],
    "payloadLen": [{ "min": 0, "max": 65536, "increment": 8 }],
    "aadLen": [{ "min": 0, "max": 65536, "increment": 8 }],
    "tagLen": [32, 64, 96, 104, 112, 120, 128],
    "ivLen": [96],
    "ivGen": ["external"]
  },
  {
    "algorithm": "HMAC-SHA-1",
    "revision": "1.0",
    "keyLen": [{ "min": 8, "max": 524288, "increment": 8 }],
    "macLen": [{ "min": 32, "max": 160, "increment": 8 }]
  },
  {
    "algorithm": "HMAC-SHA2-224",
    "revision": "1.0",
    "keyLen": [{ "min": 8, "max": 524288, "increment": 8 }],
    "macLen": [{ "min": 32, "max": 224, "increment": 8 }]
  },
  {
    "algorithm": "HMAC-SHA2-256",
    "revision": "1.0",
    "keyLen": [{ "min": 8, "max": 524288, "increment": 8 }],
    "macLen": [{ "min": 32, "max": 256, "increment": 8 }]
  },
  {
    "algorithm": "HMAC-SHA2-384",
    "revision": "1.0",
    "keyLen": [{ "min": 8, "max": 524288, "increment": 8 }],
    "macLen": [{ "min": 32, "max": 384, "increment": 8 }]
  },
  {
    "algorithm": "HMAC-SHA2-512",
    "revision": "1.0",
    "keyLen": [{ "min": 8, "max": 524288, "increment": 8 }],
    "macLen": [{ "min": 32, "max": 512, "increment": 8 }]
  },
  {
    "algorithm": "HMAC-SHA2-512/224",
    "revision": "1.0",
    "keyLen": [{ "min": 8, "max": 524288, "increment": 8 }],
    "macLen": [{ "min": 32, "max": 224, "increment": 8 }]
  },
  {
    "algorithm": "HMAC-SHA2-512/256",
    "revision": "1.0",
    "keyLen": [{ "min": 8, "max": 524288, "increment": 8 }],
    "macLen": [{ "min": 32, "max": 256, "increment": 8 }]
  },
  {
    "algorithm": "HMAC-SHA3-224",
    "revision": "1.0",
    "keyLen": [{ "min": 8, "max": 524288, "increment": 8 }],
    "macLen": [{ "min": 32, "max": 224, "increment": 8 }]
  },
  {
    "algorithm": "HMAC-SHA3-256",
    "revision": "1.0",
    "keyLen": [{ "min": 8, "max": 524288, "increment": 8 }],
    "macLen": [{ "min": 32, "max": 256, "increment": 8 }]
  },
  {
    "algorithm": "HMAC-SHA3-384",
    "revision": "1.0",
    "keyLen": [{ "min": 8, "max": 524288, "increment": 8 }],
    "macLen": [{ "min": 32, "max": 384, "increment": 8 }]
  },
  {
    "algorithm": "HMAC-SHA3-512",
    "revision": "1.0",
    "keyLen": [{ "min": 8, "max": 524288, "increment": 8 }],
    "macLen": [{ "min": 32, "max": 512, "increment": 8 }]
  },
  {
    "algorithm": "SHAKE-128",
    "revision": "1.0",
    "inBit": false,
    "outBit": false,
    "inEmpty": true,
    "outputLen": [{ "min": 16, "max": 65536, "increment": 8 }]
  },
  {
    "algorithm": "SHAKE-256",
    "revision": "1.0",
    "inBit": false,
    "outBit": false,
    "inEmpty": true,
    "outputLen": [{ "min": 16, "max": 65536, "increment": 8 }]
  },
  {
    "algorithm": "cSHAKE-128",
    "revision": "1.0",
    "hexCustomization": false,
    "outputLen": [{ "min": 16, "max": 65536, "increment": 8 }],
    "msgLen": [{ "min": 0, "max": 65536, "increment": 8 }]
  },
  {
    "algorithm": "cSHAKE-256",
    "revision": "1.0",
    "hexCustomization": false,
    "outputLen": [{ "min": 16, "max": 65536, "increment": 8 }],
    "msgLen": [{ "min": 0, "max": 65536, "increment": 8 }]
  },
  {
    "algorithm": "PBKDF",
    "revision": "1.0",
    "capabilities": [{
      "iterationCount": [{ "min": 1, "max": 10000, "increment": 1 }],
      "keyLen": [{ "min": 112, "max": 4096, "increment": 8 }],
      "passwordLen": [{ "min": 8, "max": 64, "increment": 1 }],
      "saltLen": [{ "min": 128, "max": 512, "increment": 8 }],
      "hmacAlg": [
        "SHA2-224", "SHA2-256", "SHA2-384", "SHA2-512",
        "SHA2-512/224", "SHA2-512/256",
        "SHA3-224", "SHA3-256", "SHA3-384", "SHA3-512"
      ]
    }]
  },
  {
    "algorithm": "hmacDRBG",
    "revision": "1.0",
    "predResistanceEnabled": [false],
    "reseedImplemented": false,
    "capabilities": [{
      "mode": "SHA2-224",
      "derFuncEnabled": false,
      "entropyInputLen": [192],
      "nonceLen": [96],
      "persoStringLen": [192],
      "additionalInputLen": [0],
      "returnedBitsLen": 224
    }, {
      "mode": "SHA2-256",
      "derFuncEnabled": false,
      "entropyInputLen": [256],
      "nonceLen": [128],
      "persoStringLen": [256],
      "additionalInputLen": [0],
      "returnedBitsLen": 256
    }, {
      "mode": "SHA2-384",
      "derFuncEnabled": false,
      "entropyInputLen": [256],
      "nonceLen": [128],
      "persoStringLen": [256],
      "additionalInputLen": [0],
      "returnedBitsLen": 384
    }, {
      "mode": "SHA2-512",
      "derFuncEnabled": false,
      "entropyInputLen": [256],
      "nonceLen": [128],
      "persoStringLen": [256],
      "additionalInputLen": [0],
      "returnedBitsLen": 512
    }, {
      "mode": "SHA2-512/224",
      "derFuncEnabled": false,
      "entropyInputLen": [192],
      "nonceLen": [96],
      "persoStringLen": [192],
      "additionalInputLen": [0],
      "returnedBitsLen": 224
    }, {
      "mode": "SHA2-512/256",
      "derFuncEnabled": false,
      "entropyInputLen": [256],
      "nonceLen": [128],
      "persoStringLen": [256],
      "additionalInputLen": [0],
      "returnedBitsLen": 256
    }, {
      "mode": "SHA3-224",
      "derFuncEnabled": false,
      "entropyInputLen": [192],
      "nonceLen": [96],
      "persoStringLen": [192],
      "additionalInputLen": [0],
      "returnedBitsLen": 224
    }, {
      "mode": "SHA3-256",
      "derFuncEnabled": false,
      "entropyInputLen": [256],
      "nonceLen": [128],
      "persoStringLen": [256],
      "additionalInputLen": [0],
      "returnedBitsLen": 256
    }, {
      "mode": "SHA3-384",
      "derFuncEnabled": false,
      "entropyInputLen": [256],
      "nonceLen": [128],
      "persoStringLen": [256],
      "additionalInputLen": [0],
      "returnedBitsLen": 384
    }, {
      "mode": "SHA3-512",
      "derFuncEnabled": false,
      "entropyInputLen": [256],
      "nonceLen": [128],
      "persoStringLen": [256],
      "additionalInputLen": [0],
      "returnedBitsLen": 512
    }]
  },
  {
    "algorithm": "KDA",
    "mode": "HKDF",
    "revision": "Sp800-56Cr1",
    "fixedInfoPattern": "uPartyInfo||vPartyInfo",
    "encoding": ["concatenation"],
    "hmacAlg": [
      "SHA2-224", "SHA2-256", "SHA2-384", "SHA2-512",
      "SHA2-512/224", "SHA2-512/256",
      "SHA3-224", "SHA3-256", "SHA3-384", "SHA3-512"
    ],
    "macSaltMethods": ["default", "random"],
    "l": 2048,
    "z": [{ "min": 224, "max": 65336, "increment": 8 }]
  },
  {
    "algorithm": "ctrDRBG",
    "revision": "1.0",
    "predResistanceEnabled": [false],
    "reseedImplemented": true,
    "capabilities": [{
      "mode": "AES-256",
      "derFuncEnabled": false,
      "entropyInputLen": [384],
      "nonceLen": [0],
      "persoStringLen": [0],
      "additionalInputLen": [384],
      "returnedBitsLen": 128
    }]
  },
  {
    "algorithm": "KDA",
    "mode": "OneStepNoCounter",
    "revision": "Sp800-56Cr2",
    "auxFunctions": [
      { "auxFunctionName": "HMAC-SHA2-224", "l": 224, "macSaltMethods": ["default", "random"] },
      { "auxFunctionName": "HMAC-SHA2-256", "l": 256, "macSaltMethods": ["default", "random"] },
      { "auxFunctionName": "HMAC-SHA2-384", "l": 384, "macSaltMethods": ["default", "random"] },
      { "auxFunctionName": "HMAC-SHA2-512", "l": 512, "macSaltMethods": ["default", "random"] },
      { "auxFunctionName": "HMAC-SHA2-512/224", "l": 224, "macSaltMethods": ["default", "random"] },
      { "auxFunctionName": "HMAC-SHA2-512/256", "l": 256, "macSaltMethods": ["default", "random"] },
      { "auxFunctionName": "HMAC-SHA3-224", "l": 224, "macSaltMethods": ["default", "random"] },
      { "auxFunctionName": "HMAC-SHA3-256", "l": 256, "macSaltMethods": ["default", "random"] },
      { "auxFunctionName": "HMAC-SHA3-384", "l": 384, "macSaltMethods": ["default", "random"] },
      { "auxFunctionName": "HMAC-SHA3-512", "l": 512, "macSaltMethods": ["default", "random"] }
    ],
    "fixedInfoPattern": "uPartyInfo||vPartyInfo",
    "encoding": ["concatenation"],
    "z": [{ "min": 224, "max": 65336, "increment": 8 }]
  },
  {
    "algorithm": "EDDSA",
    "mode": "keyGen",
    "revision": "1.0",
    "curve": ["ED-25519"]
  },
  {
    "algorithm": "EDDSA",
    "mode": "keyVer",
    "revision": "1.0",
    "curve": ["ED-25519"]
  },
  {
    "algorithm": "EDDSA",
    "mode": "sigGen",
    "revision": "1.0",
    "pure": true,
    "preHash": true,
    "contextLength": [{ "min": 0, "max": 255, "increment": 1 }],
    "curve": ["ED-25519"]
  },
  {
    "algorithm": "EDDSA",
    "mode": "sigVer",
    "revision": "1.0",
    "pure": true,
    "preHash": true,
    "curve": ["ED-25519"]
  },
  {
    "algorithm": "ECDSA",
    "mode": "keyGen",
    "revision": "FIPS186-5",
    "curve": ["P-224", "P-256", "P-384", "P-521"],
    "secretGenerationMode": ["testing candidates"]
  },
  {
    "algorithm": "ECDSA",
    "mode": "keyVer",
    "revision": "FIPS186-5",
    "curve": ["P-224", "P-256", "P-384", "P-521"]
  },
  {
    "algorithm": "ECDSA",
    "mode": "sigGen",
    "revision": "FIPS186-5",
    "capabilities": [{
      "curve": ["P-224", "P-256", "P-384", "P-521"],
      "hashAlg": ["SHA2-224", "SHA2-256", "SHA2-384", "SHA2-512", "SHA2-512/224", "SHA2-512/256", "SHA3-224", "SHA3-256", "SHA3-384", "SHA3-512"]
    }]
  },
  {
    "algorithm": "ECDSA",
    "mode": "sigVer",
    "revision": "FIPS186-5",
    "capabilities": [{
      "curve": ["P-224", "P-256", "P-384", "P-521"],
      "hashAlg": ["SHA2-224", "SHA2-256", "SHA2-384", "SHA2-512", "SHA2-512/224", "SHA2-512/256", "SHA3-224", "SHA3-256", "SHA3-384", "SHA3-512"]
    }]
  },
  {
    "algorithm": "DetECDSA",
    "mode": "sigGen",
    "revision": "FIPS186-5",
    "capabilities": [{
      "curve": ["P-224", "P-256", "P-384", "P-521"],
      "hashAlg": ["SHA2-224", "SHA2-256", "SHA2-384", "SHA2-512", "SHA2-512/224", "SHA2-512/256", "SHA3-224", "SHA3-256", "SHA3-384", "SHA3-512"]
    }]
  },
  {
    "algorithm": "CMAC-AES",
    "revision": "1.0",
    "capabilities": [{
      "direction": ["gen", "ver"],
      "msgLen": [{ "min": 0, "max": 524288, "increment": 8 }],
      "keyLen": [128, 256],
      "macLen": [128]
    }]
  },
  {
    "algorithm": "TLS-v1.2",
    "mode": "KDF",
    "revision": "RFC7627",
    "hashAlg": ["SHA2-256", "SHA2-384", "SHA2-512"]
  },
  {
    "algorithm": "TLS-v1.3",
    "mode": "KDF",
    "revision": "RFC8446",
    "hmacAlg": ["SHA2-256", "SHA2-384"],
    "runningMode": ["DHE", "PSK", "PSK-DHE"]
  },
  {
    "algorithm": "kdf-components",
    "mode": "ssh",
    "revision": "1.0",
    "hashAlg": ["SHA2-224", "SHA2-256", "SHA2-384", "SHA2-512"],
    "cipher": ["AES-128", "AES-192", "AES-256"]
  },
  {
    "algorithm": "KAS-ECC-SSC",
    "revision": "Sp800-56Ar3",
    "scheme": {
      "ephemeralUnified": { "kasRole": ["initiator", "responder"] },
      "staticUnified": { "kasRole": ["initiator", "responder"] }
    },
    "domainParameterGenerationMethods": ["P-224", "P-256", "P-384", "P-521"]
  },
  {
    "algorithm": "KDF",
    "revision": "1.0",
    "capabilities": [{
      "kdfMode": "counter",
      "macMode": ["CMAC-AES128", "CMAC-AES192", "CMAC-AES256"],
      "supportedLengths": [256],
      "fixedDataOrder": ["before fixed data"],
      "counterLength": [16]
    }, {
      "kdfMode": "feedback",
      "macMode": ["HMAC-SHA2-224", "HMAC-SHA2-256", "HMAC-SHA2-384", "HMAC-SHA2-512", "HMAC-SHA2-512/224", "HMAC-SHA2-512/256", "HMAC-SHA3-224", "HMAC-SHA3-256", "HMAC-SHA3-384", "HMAC-SHA3-512"],
      "customKeyInLength": 0,
      "supportedLengths": [{ "min": 8, "max": 4096, "increment": 8 }],
      "fixedDataOrder": ["after fixed data"],
      "counterLength": [8],
      "supportsEmptyIv": true,
      "requiresEmptyIv": true
    }]
  },
  {
    "algorithm": "RSA",
    "mode": "keyGen",
    "revision": "FIPS186-5",
    "infoGeneratedByServer": true,
    "pubExpMode": "fixed",
    "fixedPubExp": "010001",
    "keyFormat": "standard",
    "capabilities": [{
      "randPQ": "probable",
      "properties": [
        { "modulo": 2048, "primeTest": ["2powSecStr"] },
        { "modulo": 3072, "primeTest": ["2powSecStr"] },
        { "modulo": 4096, "primeTest": ["2powSecStr"] }
      ]
    }]
  },
  {
    "algorithm": "RSA",
    "mode": "sigGen",
    "revision": "FIPS186-5",
    "capabilities": [{
      "sigType": "pkcs1v1.5",
      "properties": [
        { "modulo": 2048, "hashPair": [{ "hashAlg": "SHA2-224" }, { "hashAlg": "SHA2-256" }, { "hashAlg": "SHA2-384" }, { "hashAlg": "SHA2-512" }] },
        { "modulo": 3072, "hashPair": [{ "hashAlg": "SHA2-224" }, { "hashAlg": "SHA2-256" }, { "hashAlg": "SHA2-384" }, { "hashAlg": "SHA2-512" }] },
        { "modulo": 4096, "hashPair": [{ "hashAlg": "SHA2-224" }, { "hashAlg": "SHA2-256" }, { "hashAlg": "SHA2-384" }, { "hashAlg": "SHA2-512" }] }
      ]
    }, {
      "sigType": "pss",
      "properties": [
        { "maskFunction": ["mgf1"], "modulo": 2048, "hashPair": [{ "hashAlg": "SHA2-224", "saltLen": 28 }, { "hashAlg": "SHA2-256", "saltLen": 32 }, { "hashAlg": "SHA2-384", "saltLen": 48 }, { "hashAlg": "SHA2-512", "saltLen": 64 }] },
        { "maskFunction": ["mgf1"], "modulo": 3072, "hashPair": [{ "hashAlg": "SHA2-224", "saltLen": 28 }, { "hashAlg": "SHA2-256", "saltLen": 32 }, { "hashAlg": "SHA2-384", "saltLen": 48 }, { "hashAlg": "SHA2-512", "saltLen": 64 }] },
        { "maskFunction": ["mgf1"], "modulo": 4096, "hashPair": [{ "hashAlg": "SHA2-224", "saltLen": 28 }, { "hashAlg": "SHA2-256", "saltLen": 32 }, { "hashAlg": "SHA2-384", "saltLen": 48 }, { "hashAlg": "SHA2-512", "saltLen": 64 }] }
      ]
    }]
  },
  {
    "algorithm": "RSA",
    "mode": "sigVer",
    "revision": "FIPS186-5",
    "pubExpMode": "fixed",
    "fixedPubExp": "010001",
    "capabilities": [{
      "sigType": "pkcs1v1.5",
      "properties": [{ "modulo": 2048, "hashPair": [{ "hashAlg": "SHA2-224" }, { "hashAlg": "SHA2-256" }, { "hashAlg": "SHA2-384" }, { "hashAlg": "SHA2-512" }] }]
    }, {
      "sigType": "pkcs1v1.5",
      "properties": [{ "modulo": 3072, "hashPair": [{ "hashAlg": "SHA2-224" }, { "hashAlg": "SHA2-256" }, { "hashAlg": "SHA2-384" }, { "hashAlg": "SHA2-512" }] }]
    }, {
      "sigType": "pkcs1v1.5",
      "properties": [{ "modulo": 4096, "hashPair": [{ "hashAlg": "SHA2-224" }, { "hashAlg": "SHA2-256" }, { "hashAlg": "SHA2-384" }, { "hashAlg": "SHA2-512" }] }]
    }, {
      "sigType": "pss",
      "properties": [{ "maskFunction": ["mgf1"], "modulo": 2048, "hashPair": [{ "hashAlg": "SHA2-224", "saltLen": 28 }, { "hashAlg": "SHA2-256", "saltLen": 32 }, { "hashAlg": "SHA2-384", "saltLen": 48 }, { "hashAlg": "SHA2-512", "saltLen": 64 }] }]
    }, {
      "sigType": "pss",
      "properties": [{ "maskFunction": ["mgf1"], "modulo": 3072, "hashPair": [{ "hashAlg": "SHA2-224", "saltLen": 28 }, { "hashAlg": "SHA2-256", "saltLen": 32 }, { "hashAlg": "SHA2-384", "saltLen": 48 }, { "hashAlg": "SHA2-512", "saltLen": 64 }] }]
    }, {
      "sigType": "pss",
      "properties": [{ "maskFunction": ["mgf1"], "modulo": 4096, "hashPair": [{ "hashAlg": "SHA2-224", "saltLen": 28 }, { "hashAlg": "SHA2-256", "saltLen": 32 }, { "hashAlg": "SHA2-384", "saltLen": 48 }, { "hashAlg": "SHA2-512", "saltLen": 64 }] }]
    }]
  }
]`)

var commands = map[string]command{
	"getConfig": {
		handler: func(args [][]byte) ([][]byte, error) {
			return [][]byte{capabilitiesJSON}, nil
		},
	},

	"SHA-1":            cmdHashAFT(sha1.New),
	"SHA-1/MCT":        cmdHashMCT(sha1.New),
	"SHA-1/LDT":        cmdHashLDT(sha1.New),
	"SHA2-224":         cmdHashAFT(sha256.New224),
	"SHA2-224/MCT":     cmdHashMCT(sha256.New224),
	"SHA2-224/LDT":     cmdHashLDT(sha256.New224),
	"SHA2-256":         cmdHashAFT(sha256.New),
	"SHA2-256/MCT":     cmdHashMCT(sha256.New),
	"SHA2-256/LDT":     cmdHashLDT(sha256.New),
	"SHA2-384":         cmdHashAFT(sha512.New384),
	"SHA2-384/MCT":     cmdHashMCT(sha512.New384),
	"SHA2-384/LDT":     cmdHashLDT(sha512.New384),
	"SHA2-512":         cmdHashAFT(sha512.New),
	"SHA2-512/MCT":     cmdHashMCT(sha512.New),
	"SHA2-512/LDT":     cmdHashLDT(sha512.New),
	"SHA2-512/224":     cmdHashAFT(sha512.New512_224),
	"SHA2-512/224/MCT": cmdHashMCT(sha512.New512_224),
	"SHA2-512/224/LDT": cmdHashLDT(sha512.New512_224),
	"SHA2-512/256":     cmdHashAFT(sha512.New512_256),
	"SHA2-512/256/MCT": cmdHashMCT(sha512.New512_256),
	"SHA2-512/256/LDT": cmdHashLDT(sha512.New512_256),

	"SHA3-224":     cmdHashAFT(newSHA3224),
	"SHA3-224/MCT": cmdSha3MCT(newSHA3224),
	"SHA3-224/LDT": cmdHashLDT(newSHA3224),
	"SHA3-256":     cmdHashAFT(newSHA3256),
	"SHA3-256/MCT": cmdSha3MCT(newSHA3256),
	"SHA3-256/LDT": cmdHashLDT(newSHA3256),
	"SHA3-384":     cmdHashAFT(newSHA3384),
	"SHA3-384/MCT": cmdSha3MCT(newSHA3384),
	"SHA3-384/LDT": cmdHashLDT(newSHA3384),
	"SHA3-512":     cmdHashAFT(newSHA3512),
	"SHA3-512/MCT": cmdSha3MCT(newSHA3512),
	"SHA3-512/LDT": cmdHashLDT(newSHA3512),
	"SHAKE-128":    cmdShakeAFTVOT(sha3.NewSHAKE128),
	"SHAKE-128/VOT": cmdShakeAFTVOT(
		sha3.NewSHAKE128,
	),
	"SHAKE-128/MCT": cmdShakeMCT(sha3.NewSHAKE128),
	"SHAKE-256":     cmdShakeAFTVOT(sha3.NewSHAKE256),
	"SHAKE-256/VOT": cmdShakeAFTVOT(sha3.NewSHAKE256),
	"SHAKE-256/MCT": cmdShakeMCT(sha3.NewSHAKE256),
	"cSHAKE-128": cmdCShakeAFT(func(N, S []byte) *sha3.SHAKE {
		return sha3.NewCSHAKE128(N, S)
	}),
	"cSHAKE-128/MCT": cmdCShakeMCT(func(N, S []byte) *sha3.SHAKE {
		return sha3.NewCSHAKE128(N, S)
	}),
	"cSHAKE-256": cmdCShakeAFT(func(N, S []byte) *sha3.SHAKE {
		return sha3.NewCSHAKE256(N, S)
	}),
	"cSHAKE-256/MCT": cmdCShakeMCT(func(N, S []byte) *sha3.SHAKE {
		return sha3.NewCSHAKE256(N, S)
	}),

	"HMAC-SHA2-224":            cmdHMAC(sha256.New224),
	"HMAC-SHA-1":               cmdHMAC(sha1.New),
	"HMAC-SHA2-256":            cmdHMAC(sha256.New),
	"HMAC-SHA2-384":            cmdHMAC(sha512.New384),
	"HMAC-SHA2-512":            cmdHMAC(sha512.New),
	"HMAC-SHA2-512/224":        cmdHMAC(sha512.New512_224),
	"HMAC-SHA2-512/256":        cmdHMAC(sha512.New512_256),
	"HMAC-SHA2-512-224":        cmdHMAC(sha512.New512_224),
	"HMAC-SHA2-512-256":        cmdHMAC(sha512.New512_256),
	"HMAC-SHA3-224":            cmdHMAC(newSHA3224),
	"HMAC-SHA3-256":            cmdHMAC(newSHA3256),
	"HMAC-SHA3-384":            cmdHMAC(newSHA3384),
	"HMAC-SHA3-512":            cmdHMAC(newSHA3512),
	"HKDF/SHA2-224":            cmdHKDFAFT(sha256.New224),
	"HKDF/SHA2-256":            cmdHKDFAFT(sha256.New),
	"HKDF/SHA2-384":            cmdHKDFAFT(sha512.New384),
	"HKDF/SHA2-512":            cmdHKDFAFT(sha512.New),
	"HKDF/SHA2-512/224":        cmdHKDFAFT(sha512.New512_224),
	"HKDF/SHA2-512/256":        cmdHKDFAFT(sha512.New512_256),
	"HKDF/SHA3-224":            cmdHKDFAFT(newSHA3224),
	"HKDF/SHA3-256":            cmdHKDFAFT(newSHA3256),
	"HKDF/SHA3-384":            cmdHKDFAFT(newSHA3384),
	"HKDF/SHA3-512":            cmdHKDFAFT(newSHA3512),
	"HKDFExtract/SHA2-256":     cmdHKDFExtractAFT(sha256.New),
	"HKDFExtract/SHA2-384":     cmdHKDFExtractAFT(sha512.New384),
	"HKDFExpandLabel/SHA2-256": cmdHKDFExpandLabelAFT(sha256.New),
	"HKDFExpandLabel/SHA2-384": cmdHKDFExpandLabelAFT(sha512.New384),
	"PBKDF":                    cmdPBKDF(),
	"hmacDRBG/SHA2-224":        cmdHMACDRBGAFT(sha256.New224),
	"hmacDRBG/SHA2-256":        cmdHMACDRBGAFT(sha256.New),
	"hmacDRBG/SHA2-384":        cmdHMACDRBGAFT(sha512.New384),
	"hmacDRBG/SHA2-512":        cmdHMACDRBGAFT(sha512.New),
	"hmacDRBG/SHA2-512/224":    cmdHMACDRBGAFT(sha512.New512_224),
	"hmacDRBG/SHA2-512/256":    cmdHMACDRBGAFT(sha512.New512_256),
	"hmacDRBG/SHA3-224":        cmdHMACDRBGAFT(newSHA3224),
	"hmacDRBG/SHA3-256":        cmdHMACDRBGAFT(newSHA3256),
	"hmacDRBG/SHA3-384":        cmdHMACDRBGAFT(newSHA3384),
	"hmacDRBG/SHA3-512":        cmdHMACDRBGAFT(newSHA3512),
	"ctrDRBG/AES-256":          cmdCTRDRBGAFT(),
	"ctrDRBG-reseed/AES-256":   cmdCTRDRBGReseedAFT(),
	"hmacDRBG-reseed/SHA2-224": cmdHMACDRBGReseed(sha256.New224),
	"hmacDRBG-reseed/SHA2-256": cmdHMACDRBGReseed(sha256.New),
	"hmacDRBG-reseed/SHA2-384": cmdHMACDRBGReseed(sha512.New384),
	"hmacDRBG-reseed/SHA2-512": cmdHMACDRBGReseed(sha512.New),
	"hmacDRBG-pr/SHA2-224":     cmdHMACDRBGPredictionResistance(sha256.New224),
	"hmacDRBG-pr/SHA2-256":     cmdHMACDRBGPredictionResistance(sha256.New),
	"hmacDRBG-pr/SHA2-384":     cmdHMACDRBGPredictionResistance(sha512.New384),
	"hmacDRBG-pr/SHA2-512":     cmdHMACDRBGPredictionResistance(sha512.New),

	"EDDSA/keyGen": cmdEDDSAKeyGen(),
	"EDDSA/keyVer": cmdEDDSAKeyVer(),
	"EDDSA/sigGen": cmdEDDSASigGen(),
	"EDDSA/sigVer": cmdEDDSASigVer(),

	"ECDSA/keyGen":    cmdECDSAKeyGen(),
	"ECDSA/keyVer":    cmdECDSAKeyVer(),
	"ECDSA/sigGen":    cmdECDSASigGen(false),
	"DetECDSA/sigGen": cmdECDSASigGen(true),
	"ECDSA/sigVer":    cmdECDSASigVer(),

	"CMAC-AES":        cmdCMACAESAFT(),
	"CMAC-AES/verify": cmdCMACAESVerifyAFT(),
	"KDF-counter":     cmdKDFCounter(),
	"KDF-feedback":    cmdKDFFeedback(),

	"OneStepNoCounter/HMAC-SHA2-224":     cmdOneStepNoCounterHMAC(sha256.New224),
	"OneStepNoCounter/HMAC-SHA2-256":     cmdOneStepNoCounterHMAC(sha256.New),
	"OneStepNoCounter/HMAC-SHA2-384":     cmdOneStepNoCounterHMAC(sha512.New384),
	"OneStepNoCounter/HMAC-SHA2-512":     cmdOneStepNoCounterHMAC(sha512.New),
	"OneStepNoCounter/HMAC-SHA2-512/224": cmdOneStepNoCounterHMAC(sha512.New512_224),
	"OneStepNoCounter/HMAC-SHA2-512/256": cmdOneStepNoCounterHMAC(sha512.New512_256),
	"OneStepNoCounter/HMAC-SHA2-512-224": cmdOneStepNoCounterHMAC(sha512.New512_224),
	"OneStepNoCounter/HMAC-SHA2-512-256": cmdOneStepNoCounterHMAC(sha512.New512_256),
	"OneStepNoCounter/HMAC-SHA3-224":     cmdOneStepNoCounterHMAC(newSHA3224),
	"OneStepNoCounter/HMAC-SHA3-256":     cmdOneStepNoCounterHMAC(newSHA3256),
	"OneStepNoCounter/HMAC-SHA3-384":     cmdOneStepNoCounterHMAC(newSHA3384),
	"OneStepNoCounter/HMAC-SHA3-512":     cmdOneStepNoCounterHMAC(newSHA3512),

	"TLSKDF/1.2/SHA2-256":    cmdTLSKDF12(sha256.New),
	"TLSKDF/1.2/SHA2-384":    cmdTLSKDF12(sha512.New384),
	"TLSKDF/1.2/SHA2-512":    cmdTLSKDF12(sha512.New),
	"SSHKDF/SHA2-224/client": cmdSSHKDF(sha256.New224, true),
	"SSHKDF/SHA2-224/server": cmdSSHKDF(sha256.New224, false),
	"SSHKDF/SHA2-256/client": cmdSSHKDF(sha256.New, true),
	"SSHKDF/SHA2-256/server": cmdSSHKDF(sha256.New, false),
	"SSHKDF/SHA2-384/client": cmdSSHKDF(sha512.New384, true),
	"SSHKDF/SHA2-384/server": cmdSSHKDF(sha512.New384, false),
	"SSHKDF/SHA2-512/client": cmdSSHKDF(sha512.New, true),
	"SSHKDF/SHA2-512/server": cmdSSHKDF(sha512.New, false),

	"ECDH/P-224": cmdECDH(elliptic.P224()),
	"ECDH/P-256": cmdECDH(elliptic.P256()),
	"ECDH/P-384": cmdECDH(elliptic.P384()),
	"ECDH/P-521": cmdECDH(elliptic.P521()),

	"RSA/keyGen":                    cmdRSAKeyGen(),
	"RSA/sigGen/SHA-1/pkcs1v1.5":    cmdRSASigGenByHashName("SHA-1", false),
	"RSA/sigGen/SHA2-224/pkcs1v1.5": cmdRSASigGenByHashName("SHA2-224", false),
	"RSA/sigGen/SHA2-256/pkcs1v1.5": cmdRSASigGenByHashName("SHA2-256", false),
	"RSA/sigGen/SHA2-384/pkcs1v1.5": cmdRSASigGenByHashName("SHA2-384", false),
	"RSA/sigGen/SHA2-512/pkcs1v1.5": cmdRSASigGenByHashName("SHA2-512", false),
	"RSA/sigGen/SHA2-224/pss":       cmdRSASigGenByHashName("SHA2-224", true),
	"RSA/sigGen/SHA2-256/pss":       cmdRSASigGenByHashName("SHA2-256", true),
	"RSA/sigGen/SHA2-384/pss":       cmdRSASigGenByHashName("SHA2-384", true),
	"RSA/sigGen/SHA2-512/pss":       cmdRSASigGenByHashName("SHA2-512", true),
	"RSA/sigVer/SHA-1/pkcs1v1.5":    cmdRSASigVerByHashName("SHA-1", false),
	"RSA/sigVer/SHA2-224/pkcs1v1.5": cmdRSASigVerByHashName("SHA2-224", false),
	"RSA/sigVer/SHA2-256/pkcs1v1.5": cmdRSASigVerByHashName("SHA2-256", false),
	"RSA/sigVer/SHA2-384/pkcs1v1.5": cmdRSASigVerByHashName("SHA2-384", false),
	"RSA/sigVer/SHA2-512/pkcs1v1.5": cmdRSASigVerByHashName("SHA2-512", false),
	"RSA/sigVer/SHA2-224/pss":       cmdRSASigVerByHashName("SHA2-224", true),
	"RSA/sigVer/SHA2-256/pss":       cmdRSASigVerByHashName("SHA2-256", true),
	"RSA/sigVer/SHA2-384/pss":       cmdRSASigVerByHashName("SHA2-384", true),
	"RSA/sigVer/SHA2-512/pss":       cmdRSASigVerByHashName("SHA2-512", true),

	"AES/encrypt":            cmdAESECB(true),
	"AES/decrypt":            cmdAESECB(false),
	"AES-CBC/encrypt":        cmdAESCBC(true),
	"AES-CBC/decrypt":        cmdAESCBC(false),
	"AES-CTR/encrypt":        cmdAESCTR(),
	"AES-CTR/decrypt":        cmdAESCTR(),
	"AES-GCM/seal":           cmdAESGCMSeal(false),
	"AES-GCM/open":           cmdAESGCMOpen(false),
	"AES-GCM-randnonce/seal": cmdAESGCMSeal(true),
	"AES-GCM-randnonce/open": cmdAESGCMOpen(true),
}

func Run(reader io.Reader, writer io.Writer) error {
	if !fips140.Enabled() {
		return errors.New("acvp must run with GODEBUG=fips140=on (or only)")
	}
	return processingLoop(bufio.NewReader(reader), writer)
}

func processingLoop(reader io.Reader, writer io.Writer) error {
	return processingLoopWithCommands(reader, writer, commands)
}

func processingLoopWithCommands(reader io.Reader, writer io.Writer, commandSet map[string]command) error {
	for {
		req, err := readRequest(reader)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading request: %w", err)
		}

		cmd, ok := commandSet[req.name]
		if !ok {
			return fmt.Errorf("unknown command: %q", req.name)
		}
		if got := len(req.args); got != cmd.requiredArgs {
			return fmt.Errorf("command %q expected %d args, got %d", req.name, cmd.requiredArgs, got)
		}

		response, err := cmd.handler(req.args)
		if err != nil {
			return fmt.Errorf("command %q failed: %w", req.name, err)
		}
		if err := writeResponse(writer, response); err != nil {
			return fmt.Errorf("write response for %q: %w", req.name, err)
		}
	}
}

func readRequest(reader io.Reader) (*request, error) {
	var numArgs uint32
	if err := binary.Read(reader, binary.LittleEndian, &numArgs); err != nil {
		return nil, err
	}
	if numArgs == 0 {
		return nil, errors.New("invalid request: zero args")
	}
	args, err := readArgs(reader, numArgs)
	if err != nil {
		return nil, err
	}
	return &request{
		name: string(args[0]),
		args: args[1:],
	}, nil
}

func readArgs(reader io.Reader, count uint32) ([][]byte, error) {
	lengths := make([]uint32, count)
	for i := range lengths {
		if err := binary.Read(reader, binary.LittleEndian, &lengths[i]); err != nil {
			return nil, fmt.Errorf("read arg length %d: %w", i, err)
		}
	}
	args := make([][]byte, count)
	for i, length := range lengths {
		buf := make([]byte, length)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, fmt.Errorf("read arg %d data: %w", i, err)
		}
		args[i] = buf
	}
	return args, nil
}

func writeResponse(writer io.Writer, args [][]byte) error {
	numArgs := uint32(len(args))
	if err := binary.Write(writer, binary.LittleEndian, numArgs); err != nil {
		return err
	}
	for i, arg := range args {
		if err := binary.Write(writer, binary.LittleEndian, uint32(len(arg))); err != nil {
			return fmt.Errorf("write arg length %d: %w", i, err)
		}
	}
	for i, arg := range args {
		if _, err := writer.Write(arg); err != nil {
			return fmt.Errorf("write arg %d data: %w", i, err)
		}
	}
	return nil
}

func cmdHashAFT(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 1,
		handler: func(args [][]byte) ([][]byte, error) {
			h := hashFn()
			h.Write(args[0])
			return [][]byte{h.Sum(nil)}, nil
		},
	}
}

func cmdHashMCT(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 1,
		handler: func(args [][]byte) ([][]byte, error) {
			h := hashFn()
			seed := args[0]
			hSize := h.Size()
			if len(seed) != hSize {
				return nil, fmt.Errorf("seed length %d does not match digest size %d", len(seed), hSize)
			}

			buf := make([]byte, 0, hSize*3)
			buf = append(buf, seed...)
			buf = append(buf, seed...)
			buf = append(buf, seed...)

			digest := make([]byte, 0, hSize)
			for i := 0; i < 1000; i++ {
				h.Reset()
				h.Write(buf)
				digest = h.Sum(digest[:0])
				copy(buf, buf[hSize:])
				copy(buf[2*hSize:], digest)
			}
			return [][]byte{buf[2*hSize:]}, nil
		},
	}
}

func cmdSha3MCT(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 1,
		handler: func(args [][]byte) ([][]byte, error) {
			h := hashFn()
			digest := args[0]
			for i := 0; i < 1000; i++ {
				h.Reset()
				h.Write(digest)
				digest = h.Sum(nil)
			}
			return [][]byte{digest}, nil
		},
	}
}

func cmdHashLDT(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 2,
		handler: func(args [][]byte) ([][]byte, error) {
			content := args[0]
			if len(args[1]) != 8 {
				return nil, fmt.Errorf("invalid repetition count size: %d", len(args[1]))
			}
			times := binary.LittleEndian.Uint64(args[1])
			h := hashFn()
			for i := uint64(0); i < times; i++ {
				h.Write(content)
			}
			return [][]byte{h.Sum(nil)}, nil
		},
	}
}

func cmdHMAC(hashFn func() hash.Hash) command {
	return command{
		requiredArgs: 2,
		handler: func(args [][]byte) ([][]byte, error) {
			msg := args[0]
			key := args[1]
			mac := hmac.New(hashFn, key)
			mac.Write(msg)
			return [][]byte{mac.Sum(nil)}, nil
		},
	}
}

func cmdAESECB(encrypt bool) command {
	return command{
		requiredArgs: 3,
		handler: func(args [][]byte) ([][]byte, error) {
			key := args[0]
			input := args[1]
			if len(input)%aes.BlockSize != 0 {
				return nil, fmt.Errorf("input length must be a multiple of %d bytes", aes.BlockSize)
			}
			iterations, err := parseIterations(args[2])
			if err != nil {
				return nil, err
			}

			block, err := aes.NewCipher(key)
			if err != nil {
				return nil, err
			}

			result := append([]byte(nil), input...)
			prevResult := make([]byte, len(result))

			for j := uint32(0); j < iterations; j++ {
				if j == iterations-1 {
					copy(prevResult, result)
				}
				for i := 0; i < len(result); i += aes.BlockSize {
					if encrypt {
						block.Encrypt(result[i:i+aes.BlockSize], result[i:i+aes.BlockSize])
					} else {
						block.Decrypt(result[i:i+aes.BlockSize], result[i:i+aes.BlockSize])
					}
				}
			}

			return [][]byte{result, prevResult}, nil
		},
	}
}

func cmdAESCBC(encrypt bool) command {
	return command{
		requiredArgs: 4,
		handler: func(args [][]byte) ([][]byte, error) {
			key := args[0]
			input := append([]byte(nil), args[1]...)
			iv := append([]byte(nil), args[2]...)

			if len(input) == 0 || len(input)%aes.BlockSize != 0 {
				return nil, fmt.Errorf("input length must be a non-zero multiple of %d", aes.BlockSize)
			}
			if len(iv) != aes.BlockSize {
				return nil, fmt.Errorf("iv length must be %d", aes.BlockSize)
			}
			iterations, err := parseIterations(args[3])
			if err != nil {
				return nil, err
			}

			block, err := aes.NewCipher(key)
			if err != nil {
				return nil, err
			}

			result := make([]byte, len(input))
			prevResult := make([]byte, len(input))
			prevInput := make([]byte, len(input))

			for j := uint32(0); j < iterations; j++ {
				copy(prevResult, result)
				if j > 0 {
					if encrypt {
						iv = append(iv[:0], result...)
					} else {
						iv = append(iv[:0], prevInput...)
					}
				}

				modeIV := make([]byte, aes.BlockSize)
				copy(modeIV, iv)
				if encrypt {
					cipher.NewCBCEncrypter(block, modeIV).CryptBlocks(result, input)
				} else {
					cipher.NewCBCDecrypter(block, modeIV).CryptBlocks(result, input)
					prevInput = append(prevInput[:0], input...)
				}

				if j == 0 {
					input = append(input[:0], iv...)
				} else {
					input = append(input[:0], prevResult...)
				}
			}

			return [][]byte{result, prevResult}, nil
		},
	}
}

func cmdAESCTR() command {
	return command{
		requiredArgs: 4,
		handler: func(args [][]byte) ([][]byte, error) {
			key := args[0]
			input := args[1]
			counter := append([]byte(nil), args[2]...)

			if len(counter) != aes.BlockSize {
				return nil, fmt.Errorf("counter length must be %d", aes.BlockSize)
			}
			iterations, err := parseIterations(args[3])
			if err != nil {
				return nil, err
			}
			if iterations != 1 {
				return nil, errors.New("aes-ctr supports only one iteration")
			}

			block, err := aes.NewCipher(key)
			if err != nil {
				return nil, err
			}

			out := make([]byte, len(input))
			cipher.NewCTR(block, counter).XORKeyStream(out, input)
			return [][]byte{out}, nil
		},
	}
}

func cmdAESGCMSeal(randNonce bool) command {
	return command{
		requiredArgs: 5,
		handler: func(args [][]byte) ([][]byte, error) {
			tagLen := int(parseU32(args[0]))
			key := args[1]
			plaintext := args[2]
			nonce := append([]byte(nil), args[3]...)
			aad := args[4]

			block, err := aes.NewCipher(key)
			if err != nil {
				return nil, err
			}

			if randNonce {
				nonce = make([]byte, 12)
				if _, err := rand.Read(nonce); err != nil {
					return nil, err
				}
			}
			if len(nonce) != 12 {
				return nil, fmt.Errorf("invalid nonce size: got %d, need 12", len(nonce))
			}

			var sealed []byte
			if tagLen >= 12 && tagLen <= 16 {
				gcm, err := cipher.NewGCMWithTagSize(block, tagLen)
				if err != nil {
					return nil, err
				}
				sealed = gcm.Seal(nil, nonce, plaintext, aad)
			} else if tagLen >= 4 && tagLen < 12 {
				// Stdlib only supports 12..16-byte tags. Match acvptool framing by
				// generating a 16-byte tag and truncating.
				gcm, err := cipher.NewGCM(block)
				if err != nil {
					return nil, err
				}
				full := gcm.Seal(nil, nonce, plaintext, aad)
				tagStart := len(full) - gcm.Overhead()
				sealed = append(append([]byte(nil), full[:tagStart]...), full[tagStart:tagStart+tagLen]...)
			} else {
				return nil, fmt.Errorf("unsupported GCM tag size: %d", tagLen)
			}
			if randNonce {
				sealed = append(sealed, nonce...)
			}
			return [][]byte{sealed}, nil
		},
	}
}

func cmdAESGCMOpen(randNonce bool) command {
	return command{
		requiredArgs: 5,
		handler: func(args [][]byte) ([][]byte, error) {
			tagLen := int(parseU32(args[0]))
			key := args[1]
			ciphertextWithTag := append([]byte(nil), args[2]...)
			nonce := append([]byte(nil), args[3]...)
			aad := args[4]
			if randNonce {
				if len(ciphertextWithTag) < 12 {
					return [][]byte{{0}, {}}, nil
				}
				nonce = ciphertextWithTag[len(ciphertextWithTag)-12:]
				ciphertextWithTag = ciphertextWithTag[:len(ciphertextWithTag)-12]
			}

			block, err := aes.NewCipher(key)
			if err != nil {
				return nil, err
			}
			if len(nonce) != 12 {
				return [][]byte{{0}, {}}, nil
			}

			if tagLen < 12 || tagLen > 16 {
				// Keep wrapper process alive for unsupported tag sizes.
				return [][]byte{{0}, {}}, nil
			}

			gcm, err := cipher.NewGCMWithTagSize(block, tagLen)
			if err != nil {
				return nil, err
			}
			plaintext, err := gcm.Open(nil, nonce, ciphertextWithTag, aad)
			if err != nil {
				return [][]byte{{0}, {}}, nil
			}
			return [][]byte{{1}, plaintext}, nil
		},
	}
}

func parseIterations(raw []byte) (uint32, error) {
	if len(raw) != 4 {
		return 0, fmt.Errorf("invalid iteration size: %d", len(raw))
	}
	iterations := binary.LittleEndian.Uint32(raw)
	if iterations == 0 || iterations == ^uint32(0) {
		return 0, fmt.Errorf("invalid iteration count: %d", iterations)
	}
	return iterations, nil
}

func parseU32(raw []byte) uint32 {
	if len(raw) < 4 {
		padded := make([]byte, 4)
		copy(padded, raw)
		return binary.LittleEndian.Uint32(padded)
	}
	return binary.LittleEndian.Uint32(raw[:4])
}

func newSHA3224() hash.Hash {
	return sha3.New224()
}

func newSHA3256() hash.Hash {
	return sha3.New256()
}

func newSHA3384() hash.Hash {
	return sha3.New384()
}

func newSHA3512() hash.Hash {
	return sha3.New512()
}

func encodeRequest(name string, args ...[]byte) []byte {
	// test helper
	total := 4 + 4*(1+len(args)) + len(name)
	for _, arg := range args {
		total += len(arg)
	}
	buf := bytes.NewBuffer(make([]byte, 0, total))
	_ = binary.Write(buf, binary.LittleEndian, uint32(1+len(args)))
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(name)))
	for _, arg := range args {
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(arg)))
	}
	buf.WriteString(name)
	for _, arg := range args {
		buf.Write(arg)
	}
	return buf.Bytes()
}
