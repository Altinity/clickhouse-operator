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

package v1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNormalizeAccessClientCert covers the both-or-neither rule: a lone path cannot form a key
// pair. It is kept, not cleared, so the connection fails like it does for a missing file instead
// of quietly going out without a certificate.
func TestNormalizeAccessClientCert(t *testing.T) {
	cases := []struct {
		name              string
		certFile, keyFile string
		wantCert, wantKey string
	}{
		{name: "neither set stays unset"},
		{
			name:     "both set are kept",
			certFile: "/etc/client/tls.crt", keyFile: "/etc/client/tls.key",
			wantCert: "/etc/client/tls.crt", wantKey: "/etc/client/tls.key",
		},
		{name: "certificate without key is kept", certFile: "/etc/client/tls.crt", wantCert: "/etc/client/tls.crt"},
		{name: "key without certificate is kept", keyFile: "/etc/client/tls.key", wantKey: "/etc/client/tls.key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &OperatorConfig{}
			c.ClickHouse.Access.ClientCertFile = tc.certFile
			c.ClickHouse.Access.ClientKeyFile = tc.keyFile
			c.normalizeSectionClickHouseAccess()
			require.Equal(t, tc.wantCert, c.ClickHouse.Access.ClientCertFile)
			require.Equal(t, tc.wantKey, c.ClickHouse.Access.ClientKeyFile)
		})
	}
}
