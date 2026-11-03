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

package zookeeper

import "time"

const (
	defaultMaxRetriesNum               = 30
	defaultMaxConcurrentRequests int64 = 32

	defaultTimeoutConnect   = 10 * time.Second
	defaultTimeoutKeepAlive = 10 * time.Second
)

type ConnectionParams struct {
	MaxRetriesNum         int
	MaxConcurrentRequests int64
	TimeoutConnect        time.Duration
	TimeoutKeepAlive      time.Duration

	CertFile string
	KeyFile  string
	CaFile   string
	AuthFile string

	// MinTLSVersion floors the TLS handshake at this protocol version.
	// "1.2"|"1.3"|"" — empty preserves Go's default (1.2 today). Used only when
	// TLS is active (CertFile + KeyFile set).
	MinTLSVersion string

	// InsecureSkipVerify, when true, disables peer-certificate and hostname
	// verification on the ZK TLS dial. False (default) preserves the existing
	// strict-verify behavior (the ZK client always provides RootCAs + ServerName).
	// Set to true only when the cluster's security.zookeeper.verify is None.
	InsecureSkipVerify bool

	// RejectDigestAuth, when true, refuses to invoke zk.AddAuth with the
	// "digest" scheme. The ZooKeeper digest scheme uses SHA-1 password
	// hashing inside the vendored go-zookeeper library — under FIPS-compatible
	// mode the operator must not exercise that path. Set true when chopconf
	// security.policy=Enforced. Per the operator's FIPS scope specification
	// (§2 line 46 / §3 step 3).
	RejectDigestAuth bool
}

func BuildConnectionParams(_params ...*ConnectionParams) *ConnectionParams {
	var params *ConnectionParams
	if len(_params) > 0 {
		params = _params[0]
	}
	return params.Normalize()
}

func (p *ConnectionParams) Normalize() *ConnectionParams {
	if p == nil {
		// Overwrite nil pointer with struct to be returned
		p = &ConnectionParams{}
	}
	if p.MaxRetriesNum == 0 {
		p.MaxRetriesNum = defaultMaxRetriesNum
	}
	if p.MaxConcurrentRequests == 0 {
		p.MaxConcurrentRequests = defaultMaxConcurrentRequests
	}
	if p.TimeoutConnect == 0 {
		p.TimeoutConnect = defaultTimeoutConnect
	}
	if p.TimeoutKeepAlive == 0 {
		p.TimeoutKeepAlive = defaultTimeoutKeepAlive
	}
	return p
}
