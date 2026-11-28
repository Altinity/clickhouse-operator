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

package chbackup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func testTopology() *Topology {
	main := topologyCluster{
		name: "main",
		shards: []topologyShard{
			{name: "0", hosts: []string{"chi-demo-main-0-0", "chi-demo-main-0-1"}},
			{name: "1", hosts: []string{"chi-demo-main-1-0", "chi-demo-main-1-1"}},
		},
	}
	return &Topology{installation: "demo", namespace: testNamespace, clusters: []topologyCluster{main}, enabled: []topologyCluster{main}}
}

func testInstallation() *api.ClickHouseInstallation {
	return newCHI("demo")
}

func testScheme(t *testing.T) *apiMachinery.Scheme {
	t.Helper()
	scheme := apiMachinery.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	return scheme
}

func testBackup(name string) *api.ClickHouseBackup {
	chb := &api.ClickHouseBackup{}
	chb.Name = name
	chb.Namespace = testNamespace
	chb.CreationTimestamp = meta.NewTime(time.Unix(1700000000, 0))
	chb.Spec.ClickHouseInstallation = "demo"
	return chb
}

func testRestore(name string) *api.ClickHouseRestore {
	chr := &api.ClickHouseRestore{}
	chr.Name = name
	chr.Namespace = testNamespace
	chr.Spec.ClickHouseInstallation = "demo"
	chr.Spec.BackupName = "nightly-20260101-000000"
	return chr
}

func testSchedule(name string) *api.ClickHouseBackupSchedule {
	chbs := &api.ClickHouseBackupSchedule{}
	chbs.Name = name
	chbs.Namespace = testNamespace
	chbs.Spec.ClickHouseInstallation = "demo"
	chbs.Spec.Schedules = []api.BackupScheduleEntry{{Period: api.BackupPeriodDaily, Keep: 1}}
	chbs.Spec.Time = "03:00"
	return chbs
}
