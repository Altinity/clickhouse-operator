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

package v1

import (
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

func (spec *ChkSpec) GetNamespaceDomainPattern() *types.String {
	return spec.NamespaceDomainPattern
}

func (spec *ChkSpec) GetDefaults() *apiChi.Defaults {
	return spec.Defaults
}

func (spec ChkSpec) GetConfiguration() apiChi.IConfiguration {
	return spec.Configuration
}

func (spec ChkSpec) GetTemplates() *apiChi.Templates {
	return spec.Templates
}

func (spec ChkSpec) EnsureConfiguration() *Configuration {
	if spec.GetConfiguration() == nil {
		spec.Configuration = new(Configuration)
	}
	return spec.Configuration
}

// MergeFrom merges from spec
func (spec *ChkSpec) MergeFrom(from *ChkSpec, _type apiChi.MergeType) {
	if from == nil {
		return
	}

	spec.Configuration = spec.Configuration.MergeFrom(from.Configuration, _type)
	spec.Templates = spec.Templates.MergeFrom(from.Templates, _type)
}
