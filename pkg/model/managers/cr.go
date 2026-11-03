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

package managers

import (
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

type CustomResourceType string

const (
	CustomResourceCHI CustomResourceType = "chi"
	CustomResourceCHK CustomResourceType = "chk"
)

func CreateCustomResource(what CustomResourceType) any {
	switch what {
	case CustomResourceCHI:
		return createCHI()
	case CustomResourceCHK:
		return createCHK()
	default:
		return nil
	}
}

func createCHI() *api.ClickHouseInstallation {
	return &api.ClickHouseInstallation{
		TypeMeta: meta.TypeMeta{
			Kind:       api.ClickHouseInstallationCRDResourceKind,
			APIVersion: api.SchemeGroupVersion.String(),
		},
	}
}

func createCHK() *apiChk.ClickHouseKeeperInstallation {
	return &apiChk.ClickHouseKeeperInstallation{
		TypeMeta: meta.TypeMeta{
			Kind:       apiChk.ClickHouseKeeperInstallationCRDResourceKind,
			APIVersion: apiChk.SchemeGroupVersion.String(),
		},
	}
}
