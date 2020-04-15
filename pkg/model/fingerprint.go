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

package model

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"sort"
)

func serialize(obj interface{}) []byte {
	b := bytes.Buffer{}
	encoder := gob.NewEncoder(&b)
	err := encoder.Encode(obj)
	if err != nil {
		fmt.Println(`failed gob Encode`, err)
	}

	return b.Bytes()
}

func hash(b []byte) string {
	hasher := sha1.New()
	hasher.Write(b)
	return hex.EncodeToString(hasher.Sum(nil))
}

func fingerprint(obj interface{}) string {
	return hash(serialize(obj))
}

func castToSliceOfStrings(m map[string]interface{}) []string {
	res := make([]string, 0, 0)

	// Sort keys
	var keys []string
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Walk over sorted keys
	for _, key := range keys {
		res = append(res, key)

		switch m[key].(type) {
		case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			value := fmt.Sprint(m[key])
			res = append(res, value)
		case []string, []int, []int8, []int16, []int32, []int64, []uint, []uint8, []uint16, []uint32, []uint64, []float32, []float64, []interface{}:
			for _, v := range m[key].([]interface{}) {
				value := fmt.Sprint(v)
				res = append(res, value)
			}
		}
	}

	return res
}
