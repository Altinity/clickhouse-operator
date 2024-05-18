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

// ConfigFilesGeneratorOptionsClickHouse specifies options for clickhouse configuration generator
type ConfigFilesGeneratorOptionsClickHouse struct {
	RemoteServersGeneratorOptions *RemoteServersGeneratorOptions
}

// NewConfigFilesGeneratorOptionsClickHouse creates new options for clickhouse configuration generator
func NewConfigFilesGeneratorOptionsClickHouse() *ConfigFilesGeneratorOptionsClickHouse {
	return &ConfigFilesGeneratorOptionsClickHouse{}
}

// GetRemoteServersGeneratorOptions gets remote-servers generator options
func (o *ConfigFilesGeneratorOptionsClickHouse) GetRemoteServersGeneratorOptions() *RemoteServersGeneratorOptions {
	if o == nil {
		return nil
	}
	return o.RemoteServersGeneratorOptions
}

// SetRemoteServersGeneratorOptions sets remote-servers generator options
func (o *ConfigFilesGeneratorOptionsClickHouse) SetRemoteServersGeneratorOptions(opts *RemoteServersGeneratorOptions) *ConfigFilesGeneratorOptionsClickHouse {
	if o == nil {
		return nil
	}
	o.RemoteServersGeneratorOptions = opts

	return o
}

// defaultConfigFilesGeneratorOptionsClickHouse creates new default options for clickhouse config generator
func defaultConfigFilesGeneratorOptionsClickHouse() *ConfigFilesGeneratorOptionsClickHouse {
	return NewConfigFilesGeneratorOptionsClickHouse()
}
