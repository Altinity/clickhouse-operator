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

const (
	// Max number of tries for SQL queries
	defaultMaxTries = 10
)

// QueryOptions
type QueryOptions struct {
	Retry    bool
	Tries    int
	Parallel bool
	Silent   bool
	*Timeouts
}

// NewQueryOptions
func NewQueryOptions() *QueryOptions {
	opts := new(QueryOptions)
	opts.Timeouts = NewTimeouts()
	return opts
}

// QueryOptionsNormalize
func QueryOptionsNormalize(opts ...*QueryOptions) *QueryOptions {
	if len(opts) == 0 {
		return NewQueryOptions().Normalize()
	} else {
		return opts[0].Normalize()
	}
}

// Normalize
func (o *QueryOptions) Normalize() *QueryOptions {
	if o == nil {
		o = NewQueryOptions()
	}
	if (o.Tries == 0) && o.Retry {
		o.Tries = defaultMaxTries
	}
	if o.Tries == 0 {
		// We need to have at least one try
		o.Tries = 1
	}
	return o
}

// GetRetry
func (o *QueryOptions) GetRetry() bool {
	if o == nil {
		return false
	}
	return o.Retry
}

// SetRetry
func (o *QueryOptions) SetRetry(retry bool) *QueryOptions {
	if o == nil {
		return nil
	}
	o.Retry = retry
	return o
}

// GetSilent
func (o *QueryOptions) GetSilent() bool {
	if o == nil {
		return false
	}
	return o.Silent
}

// SetSilent
func (o *QueryOptions) SetSilent(silent bool) *QueryOptions {
	if o == nil {
		return nil
	}
	o.Silent = silent
	return o
}
