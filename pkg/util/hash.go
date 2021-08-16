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
	// #nosec
	// G505 (CWE-327): Blocklisted import crypto/sha1: weak cryptographic primitive
	// It is good enough for string ID
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"hash/fnv"

	dumper "github.com/sanity-io/litter"
	//	"github.com/davecgh/go-spew/spew"
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
	//s := spew.NewDefaultConfig()
	//s.SortKeys = true
	d := dumper.Options{
		Separator: " ",
	}
	return []byte(d.Sdump(obj))
}

// HashIntoString hashes bytes and returns string version of the hash
func HashIntoString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	// #nosec
	// G401 (CWE-326): Use of weak cryptographic primitive
	// It is good enough for string ID
	hasher := sha1.New()
	hasher.Write(b)
	return hex.EncodeToString(hasher.Sum(nil))
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
