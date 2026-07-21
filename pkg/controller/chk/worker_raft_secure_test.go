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

package chk

import (
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// secureHost builds a host with an explicit per-host `insecure` posture, so the
// test exercises Host.IsInsecure() without a wired cluster fallback.
func secureHost(name string, idx int, insecure bool) *api.Host {
	h := stagingHost(name, idx, types.ObjectStatusFound, true)
	h.Insecure = types.NewStringBool(insecure)
	return h
}

// TestCRIsSecureOnlyKeeper pins the secure-only detection that drives the
// Batch C fallback: the new Raft gates are skipped only when EVERY host has its
// plaintext client port closed (mirrors Generator.getPlaintextListenerRemoval).
func TestCRIsSecureOnlyKeeper(t *testing.T) {
	t.Run("all hosts plaintext-closed => secure-only", func(t *testing.T) {
		cr := stagingCR(0, secureHost("k0", 0, false), secureHost("k1", 1, false), secureHost("k2", 2, false))
		require.True(t, crIsSecureOnlyKeeper(cr))
	})

	t.Run("plaintext-open hosts => not secure-only", func(t *testing.T) {
		// A legacy CHK resolves IsInsecure()==true, so it is never mis-detected as
		// secure-only and the new Raft gates keep running for it.
		cr := stagingCR(0, secureHost("k0", 0, true), secureHost("k1", 1, true))
		require.False(t, crIsSecureOnlyKeeper(cr))
	})

	t.Run("mixed: one host still plaintext-open => not secure-only", func(t *testing.T) {
		cr := stagingCR(0, secureHost("k0", 0, false), secureHost("k1", 1, true))
		require.False(t, crIsSecureOnlyKeeper(cr))
	})
}
