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

package chk

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
)

func getCluster(chk *api.ClickHouseKeeperInstallation) *api.ChkCluster {
	return chk.Spec.GetConfiguration().GetCluster(0)
}

func GetReplicasCount(chk *api.ClickHouseKeeperInstallation) int {
	cluster := getCluster(chk)
	if cluster == nil {
		return 0
	}
	return cluster.GetLayout().GetReplicasCount()
}
