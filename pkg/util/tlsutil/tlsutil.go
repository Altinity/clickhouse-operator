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
)

// Plain-string constants for the TLS minimum-version knob. tlsutil is a
// leaf-level package (stdlib only) so it cannot import pkg/apis to reference
// the typed TLSMinVersion alias directly. The apis package owns the typed
// alias (TLSMinVersion12/13) whose string values MUST match these — callers
// convert their typed value to a plain string at the call site and pass it
// through here.
const (
	MinVersion12 = "1.2"
	MinVersion13 = "1.3"
)

// VersionUint16 maps a TLS minimum-version string ("1.2"|"1.3"|"") to the
// corresponding tls.Version* constant. Empty or unknown returns 0, which Go
// treats as "use the stdlib default" (currently 1.2). Shared by the ClickHouse
// and ZooKeeper client paths so both apply identical floor logic.
//
// Signature is plain string (not the apis TLSMinVersion alias) to keep this
// package leaf-level — see the package-level constants above for rationale.
func VersionUint16(v string) uint16 {
	switch v {
	case MinVersion12:
		return tls.VersionTLS12
	case MinVersion13:
		return tls.VersionTLS13
	default:
		return 0
	}
}
