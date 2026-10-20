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
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// The clusters of a template are shared by every installation the template applies to, and
// installations are normalized concurrently. An installation takes copies of its own before it drops
// the template's recovery and inherits its own, as the normalizer does: the template is only ever
// read, and every installation recovers from its own backup.
func TestOwnClustersLeavesTheTemplateAlone(t *testing.T) {
	const templateRecovery = "tenant-a-20261004-020000"
	template := &ClickHouseInstallation{}
	template.Spec.Configuration = &Configuration{Clusters: []*Cluster{
		{Name: "with-backup", Backup: &ClusterBackup{
			Enabled:  types.NewStringBool(true),
			Recovery: &BackupRecovery{BackupName: types.NewString(templateRecovery)},
		}},
		{Name: "without-backup"},
	}}

	const installations = 8
	recovered := make([][]string, installations)
	var wg sync.WaitGroup
	for i := range installations {
		wg.Add(1)
		go func() {
			defer wg.Done()
			own := &ClickHouseInstallation{}
			own.Spec.Backup = &ClusterBackup{Recovery: &BackupRecovery{BackupName: types.NewString("own-" + strconv.Itoa(i))}}

			target := &ClickHouseInstallation{}
			target.Spec.MergeFrom(&template.Spec, MergeTypeOverrideByNonEmptyValues)
			target.Spec.Configuration.OwnClusters()
			target.Spec.DropBackupRecovery()
			target.Spec.MergeFrom(&own.Spec, MergeTypeOverrideByNonEmptyValues)
			for _, cluster := range target.Spec.Configuration.Clusters {
				cluster.InheritClusterBackupFrom(target)
				recovered[i] = append(recovered[i], cluster.GetBackup().GetRecovery().GetBackupName().Value())
			}
		}()
	}
	wg.Wait()

	for i := range installations {
		require.Equal(t, []string{"own-" + strconv.Itoa(i), "own-" + strconv.Itoa(i)}, recovered[i])
	}
	require.Equal(t, templateRecovery, template.Spec.Configuration.Clusters[0].GetBackup().GetRecovery().GetBackupName().Value())
	require.Nil(t, template.Spec.Configuration.Clusters[1].GetBackup())
}

// A copy of a cluster is a cluster of its own that keeps every setting, without the links to the
// installation a normalization gives a cluster.
func TestOwnClustersCopiesTheClusters(t *testing.T) {
	installation := &ClickHouseInstallation{}
	cluster := &Cluster{
		Name:      "c",
		Layout:    &ChiClusterLayout{ShardsCount: 2},
		Backup:    &ClusterBackup{Enabled: types.NewStringBool(true)},
		Zookeeper: &ZookeeperConfig{Root: "/c"},
		Runtime:   ChiClusterRuntime{CHI: installation},
	}
	configuration := &Configuration{Clusters: []*Cluster{cluster, nil}}
	shared := configuration.Clusters

	configuration.OwnClusters()
	require.Len(t, configuration.Clusters, 2)
	require.Nil(t, configuration.Clusters[1])
	copied := configuration.Clusters[0]
	require.NotSame(t, cluster, copied)
	require.Same(t, cluster, shared[0], "the clusters the configuration shared stay as they are")
	require.NotSame(t, cluster.Backup, copied.Backup)
	require.NotSame(t, cluster.Layout, copied.Layout)
	require.Equal(t, "c", copied.Name)
	require.Equal(t, 2, copied.Layout.ShardsCount)
	require.True(t, copied.Backup.IsRequested())
	require.Equal(t, "/c", copied.Zookeeper.Root)
	require.Nil(t, copied.Runtime.CHI)

	var none *Configuration
	none.OwnClusters()
}
