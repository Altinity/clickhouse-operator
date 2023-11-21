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
	// #nosec
	// G505 (CWE-327): Blocklisted import crypto/sha1: weak cryptographic primitive
	// It is good enough for string ID
	"crypto/sha1"
	"encoding/hex"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// randStringBytes specifies bytes that could be used by RandString generator
const randStringBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// RandString generates random string of specified length
func RandString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = randStringBytes[rand.Intn(len(randStringBytes))]
	}
	return string(b)
}

// RandStringRange specifies random string with length in specified range
func RandStringRange(minLength, maxLength int) string {
	return RandString(rand.Intn(maxLength-minLength+1) + minLength)
}

// CreateStringID creates HEX hash ID out of a string.
// In case maxHashLen == 0 the whole hash is returned
func CreateStringID(str string, maxHashLen int) string {
	// #nosec
	// G401 (CWE-326): Use of weak cryptographic primitive
	// It is good enough for string ID
	sha := sha1.New()
	sha.Write([]byte(str))
	hash := hex.EncodeToString(sha.Sum(nil))

	if maxHashLen == 0 {
		// Explicitly requested to return everything
		return hash
	}

	if maxHashLen >= len(hash) {
		// Requested hash len is greater than we have
		// Return whole hash - everything what we have
		return hash
	}

	// Requested hash len is smaller that the hash
	// Return last part of the hash
	return hash[len(hash)-maxHashLen:]
}

// StringHead returns beginning of the string of requested length
func StringHead(str string, maxHeadLen int) string {
	if len(str) <= maxHeadLen {
		// String is shorter than head requested - return everything
		return str
	}

	// Return beginning of the string
	return str[:maxHeadLen]
}

// StringSliceContains implements contain method for string slice
func StringSliceContains(haystack []string, needle string) bool {
	for _, a := range haystack {
		if a == needle {
			return true
		}
	}
	return false
}
