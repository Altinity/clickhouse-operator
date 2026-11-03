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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetConfig(t *testing.T) {
	in := bytes.NewBuffer(encodeRequest("getConfig"))
	var out bytes.Buffer
	err := processingLoop(in, &out)
	require.NoError(t, err)

	resp, err := decodeResponse(&out)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	require.Contains(t, string(resp[0]), `"algorithm": "SHA2-256"`)
	require.Contains(t, string(resp[0]), `"algorithm": "ACVP-AES-GCM"`)
	require.NotContains(t, string(resp[0]), `"algorithm": "ML-KEM"`)
	require.NotContains(t, string(resp[0]), `"algorithm": "ML-DSA"`)
}

func TestSHA256AFT(t *testing.T) {
	msg := []byte("abc")
	in := bytes.NewBuffer(encodeRequest("SHA2-256", msg))
	var out bytes.Buffer

	err := processingLoop(in, &out)
	require.NoError(t, err)

	resp, err := decodeResponse(&out)
	require.NoError(t, err)
	require.Len(t, resp, 1)

	sum := sha256.Sum256(msg)
	require.Equal(t, hex.EncodeToString(sum[:]), hex.EncodeToString(resp[0]))
}

func decodeResponse(r io.Reader) ([][]byte, error) {
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	lengths := make([]uint32, count)
	for i := range lengths {
		if err := binary.Read(r, binary.LittleEndian, &lengths[i]); err != nil {
			return nil, err
		}
	}
	args := make([][]byte, count)
	for i, n := range lengths {
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args[i] = buf
	}
	return args, nil
}
