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

package tlsutil

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestVersionUint16 covers every cell consumed by the ClickHouse client
// (connection.go) and the ZooKeeper client (connection.go:324). Both paths feed
// VersionUint16 a plain string ("1.2"/"1.3"/...) — empty/unknown strings MUST
// land on 0 (Go stdlib default — currently TLS 1.2) so the dial path keeps
// working with no MinVersion set; flipping any cell silently re-floors every
// outbound TLS connection in the operator.
//
// String literals (not the apis TLSMinVersion alias) on purpose: this package
// is leaf-level and must not import pkg/apis.
func TestVersionUint16(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want uint16
	}{
		{"1.2 → VersionTLS12", MinVersion12, tls.VersionTLS12},
		{"1.3 → VersionTLS13", MinVersion13, tls.VersionTLS13},
		{"empty → 0 (stdlib default)", "", 0},
		{"unknown value → 0 (fail-soft to stdlib default, not panic)", "9.9", 0},
		{"legacy lowercase tls1.2 → 0 (not normalized at this level)", "tls1.2", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, VersionUint16(c.in))
		})
	}
}
