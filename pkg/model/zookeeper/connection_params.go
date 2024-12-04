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
	defaultMaxRetriesNum               = 25
	defaultMaxConcurrentRequests int64 = 32

	defaultTimeoutConnect   = 30 * time.Second
	defaultTimeoutKeepAlive = 30 * time.Second
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
