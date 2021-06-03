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
}

// NewQueryOptions
func NewQueryOptions() *QueryOptions {
	return new(QueryOptions)
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
	if o.Tries == 0 {
		if o.Retry {
			o.Tries = defaultMaxTries
		}
	}
	return o
}

// SetRetry
func (o *QueryOptions) SetRetry(retry bool) *QueryOptions {
	if o == nil {
		return nil
	}
	o.Retry = retry
	return o
}
