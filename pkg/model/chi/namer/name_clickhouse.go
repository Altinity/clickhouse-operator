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

package namer

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/common/namer/macro"
)

// createConfigMapNameHost returns a name for a ConfigMap for replica's personal config
func createConfigMapNameHost(host *api.Host) string {
	return macro.Macro(host).Line(configMapNamePatternHost)
}

// createConfigMapNameCommon returns a name for a ConfigMap for replica's common config
func createConfigMapNameCommon(chi api.ICustomResource) string {
	return macro.Macro(chi).Line(configMapNamePatternCommon)
}

// createConfigMapNameCommonUsers returns a name for a ConfigMap for replica's common users config
func createConfigMapNameCommonUsers(chi api.ICustomResource) string {
	return macro.Macro(chi).Line(configMapNamePatternCommonUsers)
}
