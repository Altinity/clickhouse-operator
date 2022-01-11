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

package clickhouse

import (
	"time"
)

const (
	defaultConnectTimeout = 10 * time.Second
	defaultQueryTimeout   = 60 * time.Second
)

// Timeouts specifies set of timeouts for a clickhouse connection
type Timeouts struct {
	// connect specifies timeout used while connection being established
	connect time.Duration
	// query specifies timeout used when running query
	query time.Duration
}

// TimeLimitedConnector specifies interface to operate with timeouts on connection
type TimeLimitedConnector interface {
	GetConnectTimeout() time.Duration
	SetConnectTimeout(timeout time.Duration)
	GetQueryTimeout() time.Duration
	SetQueryTimeout(timeout time.Duration)
}

var _ TimeLimitedConnector = &Timeouts{}

// NewTimeouts creates new set of timeouts
func NewTimeouts() *Timeouts {
	return &Timeouts{
		connect: defaultConnectTimeout,
		query:   defaultQueryTimeout,
	}
}

// GetConnectTimeout gets connect timeout
func (t *Timeouts) GetConnectTimeout() time.Duration {
	if t == nil {
		return 0
	}
	return t.connect
}

// SetConnectTimeout sets connect timeout
func (t *Timeouts) SetConnectTimeout(timeout time.Duration) {
	if t == nil {
		return
	}
	t.connect = timeout
}

// GetQueryTimeout gets query timeout
func (t *Timeouts) GetQueryTimeout() time.Duration {
	if t == nil {
		return 0
	}
	return t.query
}

// SetQueryTimeout sets query timeout
func (t *Timeouts) SetQueryTimeout(timeout time.Duration) {
	if t == nil {
		return
	}
	t.query = timeout
}
