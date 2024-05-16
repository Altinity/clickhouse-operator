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

package config

// ClickHouseConfigFilesGeneratorOptions specifies options for clickhouse configuration generator
type ClickHouseConfigFilesGeneratorOptions struct {
	RemoteServersGeneratorOptions *RemoteServersGeneratorOptions
}

// NewClickHouseConfigFilesGeneratorOptions creates new options for clickhouse configuration generator
func NewClickHouseConfigFilesGeneratorOptions() *ClickHouseConfigFilesGeneratorOptions {
	return &ClickHouseConfigFilesGeneratorOptions{}
}

// GetRemoteServersGeneratorOptions gets remote-servers generator options
func (o *ClickHouseConfigFilesGeneratorOptions) GetRemoteServersGeneratorOptions() *RemoteServersGeneratorOptions {
	if o == nil {
		return nil
	}
	return o.RemoteServersGeneratorOptions
}

// SetRemoteServersGeneratorOptions sets remote-servers generator options
func (o *ClickHouseConfigFilesGeneratorOptions) SetRemoteServersGeneratorOptions(opts *RemoteServersGeneratorOptions) *ClickHouseConfigFilesGeneratorOptions {
	if o == nil {
		return nil
	}
	o.RemoteServersGeneratorOptions = opts

	return o
}

// defaultClickHouseConfigFilesGeneratorOptions creates new default options for clickhouse config generator
func defaultClickHouseConfigFilesGeneratorOptions() *ClickHouseConfigFilesGeneratorOptions {
	return NewClickHouseConfigFilesGeneratorOptions()
}
