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

// FilesGeneratorOptionsClickHouse specifies options for clickhouse configuration generator
type FilesGeneratorOptionsKeeper struct {
	settings *api.Settings
}

// NewConfigFilesGeneratorOptionsKeeper creates new options for keeper configuration generator
func NewConfigFilesGeneratorOptionsKeeper() *FilesGeneratorOptionsKeeper {
	return &FilesGeneratorOptionsKeeper{}
}

func (o *FilesGeneratorOptionsKeeper) GetSettings() *api.Settings {
	if o == nil {
		return nil
	}
	return o.settings
}

func (o *FilesGeneratorOptionsKeeper) SetSettings(settings *api.Settings) *FilesGeneratorOptionsKeeper {
	if o == nil {
		return nil
	}
	o.settings = settings

	return o
}
