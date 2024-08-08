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

import api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

// FilesGeneratorOptions specifies options for configuration files generator
type FilesGeneratorOptions struct {
	settings *api.Settings
}

// NewFilesGeneratorOptions creates new options for configuration files generator
func NewFilesGeneratorOptions() *FilesGeneratorOptions {
	return &FilesGeneratorOptions{}
}

func (o *FilesGeneratorOptions) GetSettings() *api.Settings {
	if o == nil {
		return nil
	}
	return o.settings
}

func (o *FilesGeneratorOptions) SetSettings(settings *api.Settings) *FilesGeneratorOptions {
	if o == nil {
		return nil
	}
	o.settings = settings

	return o
}
