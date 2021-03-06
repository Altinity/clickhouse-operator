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

func HashIntoString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	hasher := sha1.New()
	hasher.Write(b)
	return hex.EncodeToString(hasher.Sum(nil))
}

func HashIntoInt(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write(b)
	return int(h.Sum32())
}

func HashIntoIntTopped(b []byte, top int) int {
	return HashIntoInt(b) % top
}
