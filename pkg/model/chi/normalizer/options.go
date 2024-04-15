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

package normalizer

// Options specifies normalization options
type Options struct {
	// WithDefaultCluster specifies whether to insert default cluster in case no cluster specified
	WithDefaultCluster bool
	// DefaultUserAdditionalIPs specifies set of additional IPs applied to default user
	DefaultUserAdditionalIPs   []string
	DefaultUserInsertHostRegex bool
}

// NewOptions creates new Options
func NewOptions() *Options {
	return &Options{
		DefaultUserInsertHostRegex: true,
	}
}
