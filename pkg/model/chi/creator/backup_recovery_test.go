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

package creator_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// A recovery configures the operator, not the sidecar: naming one, changing it or opting a cluster
// out of it changes no StatefulSet and no pod, so no host restarts for it.
func TestBackupRecoveryChangesNoPod(t *testing.T) {
	plain := normalize(t, newCR(newCluster(backedUpCluster, enabled())))
	recovering := newCR(newCluster(backedUpCluster, enabled()))
	recovering.Spec.Backup = &chi.ClusterBackup{Recovery: &chi.BackupRecovery{BackupName: types.NewString("nightly-20261004-020000")}}
	recovering = normalize(t, recovering)
	require.Equal(t, "nightly-20261004-020000", recovering.FindCluster(backedUpCluster).(*chi.Cluster).GetBackup().GetRecovery().GetBackupName().Value())
	optedOutBackup := enabled()
	optedOutBackup.Recovery = &chi.BackupRecovery{BackupName: types.NewString("")}
	optedOut := newCR(newCluster(backedUpCluster, optedOutBackup))
	optedOut = normalize(t, optedOut)

	want, _ := createStatefulSet(t, plain, backedUpCluster)
	for name, cr := range map[string]*chi.ClickHouseInstallation{"a recovery": recovering, "an opt-out": optedOut} {
		got, _ := createStatefulSet(t, cr, backedUpCluster)
		require.Equal(t, want.Spec, got.Spec, name)
		require.Equal(t, want.Annotations, got.Annotations, name)
		require.Equal(t, want.Labels, got.Labels, name)
	}
}
