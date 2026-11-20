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
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// validatedRestore returns a restore of the test installation validated on the given clusters, of one cluster when
// cluster is set, of every cluster otherwise, that restores access entities when rbac is set: a restore of one cluster
// does so only as the recovery restore the installation controls.
func validatedRestore(name, cluster string, rbac bool, clusters ...string) *api.ClickHouseRestore {
	chr := newRestoreCR(name)
	chr.Spec.Cluster = cluster
	if rbac {
		chr.Spec.RBAC = types.NewStringBool(true)
		if cluster != "" {
			chr.OwnerReferences = []meta.OwnerReference{{APIVersion: "clickhouse.altinity.com/v1",
				Kind: api.ClickHouseInstallationCRDResourceKind, Name: testCHI, UID: "chi-uid", Controller: ptr.To(true)}}
		}
	}
	chr.Status.Phase = api.OperationPhasePending
	chr.Status.Cluster = cluster
	for _, name := range clusters {
		chr.Status.Clusters = append(chr.Status.Clusters, api.OperationClusterStatus{Name: name, Phase: api.OperationPhasePending})
	}
	return chr
}

// A restore of access entities holds the backups of the clusters it restores only: one of every cluster holds them
// all, one of a cluster holds that cluster's alone, a finished one, or one that leaves access entities out, none.
func TestAccessRestoreGateHoldsTheClustersOfTheRestore(t *testing.T) {
	finished := validatedRestore("r-done", "", true, "hot", "cold")
	finished.Status.Phase = api.OperationPhaseCompleted
	failed := validatedRestore("r-failed", "hot", true, "hot")
	failed.Status.Phase = api.OperationPhaseFailed
	cases := []struct {
		name    string
		restore *api.ClickHouseRestore
		held    []string
	}{
		{"rbac restore of one cluster", validatedRestore("r1", "hot", true, "hot"), []string{"hot"}},
		{"rbac restore of every cluster", validatedRestore("r1", "", true, "hot", "cold"), []string{"hot", "cold"}},
		{"finished rbac restore", finished, nil},
		{"failed rbac restore", failed, nil},
		{"restore without rbac", validatedRestore("r1", "", false, "hot", "cold"), nil},
		{"rbac restore not validated yet", validatedRestore("r1", "", true), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t, nil, tc.restore)
			controller := NewBackupController(c, c, newScheme(t), newFakeCR(twoClusters(t, api.StatusCompleted)))
			for _, cluster := range []string{"hot", "cold"} {
				wait, err := controller.accessRestoreGate(testNamespace, testCHI, cluster).check(context.Background())
				require.NoError(t, err)
				if !slices.Contains(tc.held, cluster) {
					require.Nil(t, wait, "the backup of cluster %s is not held", cluster)
					continue
				}
				require.NotNil(t, wait, "the backup of cluster %s is held", cluster)
				require.Equal(t, api.BackupReasonOperationInProgress, wait.reason)
				require.Equal(t, accessRestoreInProgressMessage(testCHI, cluster, tc.restore.GetName()), wait.message)
				require.Contains(t, wait.message, "cluster "+cluster)
			}
		})
	}
}

// A restore of access entities holds the schedules of the clusters it restores only, as any restore does; one of
// every cluster, whether it restores access entities or not, holds them all.
func TestRestoreHoldsTheSchedulesOfItsClusters(t *testing.T) {
	cases := []struct {
		name    string
		restore *api.ClickHouseRestore
		held    []string
	}{
		{"rbac restore of one cluster", validatedRestore("r1", "hot", true, "hot"), []string{"hot"}},
		{"rbac restore of every cluster", validatedRestore("r1", "", true, "hot", "cold"), []string{"hot", "cold"}},
		{"restore of one cluster", validatedRestore("r1", "cold", false, "cold"), []string{"cold"}},
		{"restore of every cluster", validatedRestore("r1", "", false, "hot", "cold"), []string{"hot", "cold"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chi := twoClusters(t, api.StatusCompleted)
			c := newClient(t, nil, tc.restore)
			controller := NewScheduleController(c, c, newScheme(t), newFakeCR(chi))
			holds, err := controller.restoreHolds(context.Background(), c, newScheduleCR("s1"), chi, []string{"hot", "cold"})
			require.NoError(t, err)
			for _, cluster := range []string{"hot", "cold"} {
				if !slices.Contains(tc.held, cluster) {
					require.Nil(t, holds[cluster], "the schedule of cluster %s is not held", cluster)
					continue
				}
				require.NotNil(t, holds[cluster], "the schedule of cluster %s is held", cluster)
				require.Equal(t, api.BackupReasonSuspendedForRestore, holds[cluster].reason)
				require.Equal(t, restoreInProgress(testCHI, cluster).message, holds[cluster].message)
			}
		})
	}
}

// A backup of a cluster runs while a restore of the access entities of another cluster waits to start, and one of
// the restored cluster waits for it, telling the cluster.
func TestBackupOfAnotherClusterRunsDuringAnAccessRestore(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chi := twoClusters(t, api.StatusCompleted)
	restore := validatedRestore("r1", "hot", true, "hot")
	require.True(t, model.RestoresRBAC(restore), "a recovery restore of one cluster restores access entities")
	cold, hot := newBackupCR("b-cold"), newBackupCR("b-hot")
	cold.Spec.Cluster, hot.Spec.Cluster = "cold", "hot"
	// Storage paths of their own, which the storage path check of each cluster tells apart
	paths := func(cluster string) *core.Secret {
		return storageSettings(t, chi, cluster, map[string]string{"REMOTE_STORAGE": "s3", "S3_BUCKET": "backups",
			"S3_PATH": "backup/" + cluster + "/{shard}"})
	}
	c := newClient(t, nil, cold, hot, restore, paths("hot"), paths("cold"))
	controller := NewBackupController(c, c, newScheme(t), newFakeCR(chi))

	reconcileUntil(t, func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("b-cold")) }, func() bool {
		return getBackup(t, c, "b-cold").Status.Phase == api.OperationPhaseCompleted
	})
	for i := 0; i < 2; i++ {
		_, err := controller.Reconcile(context.Background(), request("b-hot"))
		require.NoError(t, err)
	}
	got := getBackup(t, c, "b-hot")
	require.Equal(t, api.OperationPhasePending, got.Status.Phase)
	require.Equal(t, api.BackupReasonOperationInProgress, got.Status.Clusters[0].Reason)
	require.Equal(t, accessRestoreInProgressMessage(testCHI, "hot", "r1"), got.Status.Clusters[0].Message)
}

// The recovery restores of different clusters of an installation, which restore access entities, run at the same
// time, while a restore of a cluster one of them restores waits for it.
func TestRestoresOfDifferentClustersRunTogether(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chi := twoClusters(t, api.StatusCompleted)
	for _, host := range []string{"chi-demo-hot-0-0", "chi-demo-hot-0-1", "chi-demo-hot-1-0", "chi-demo-hot-1-1",
		"chi-demo-cold-0-0", "chi-demo-cold-0-1"} {
		sc.hosts[host] = &fakeHost{remote: []backupRow{{Name: "nightly-20260101-000000", DataSize: 1, MetadataSize: 1}}, hang: "download"}
	}
	paths := func(cluster string) *core.Secret {
		return storageSettings(t, chi, cluster, map[string]string{"REMOTE_STORAGE": "s3", "S3_BUCKET": "backups",
			"S3_PATH": "backup/" + cluster + "/{shard}"})
	}
	hot, cold := validatedRestore("r-hot", "hot", true, "hot"), validatedRestore("r-cold", "cold", true, "cold")
	again := validatedRestore("r-hot-again", "hot", false, "hot")
	c := newClient(t, nil, hot, cold, again, paths("hot"), paths("cold"))
	controller := NewRestoreController(c, c, newScheme(t), newFakeCR(chi))
	reconcile := func(name string) func() (ctrl.Result, error) {
		return func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request(name)) }
	}
	downloading := func(cluster string) func() bool {
		return func() bool {
			return slices.ContainsFunc(commandsHolding(sc, "download"), func(command string) bool {
				return strings.HasPrefix(command, "chi-demo-"+cluster+"-")
			})
		}
	}

	reconcileUntil(t, reconcile("r-hot"), downloading("hot"))
	reconcileUntil(t, reconcile("r-cold"), downloading("cold"))
	require.Equal(t, api.OperationPhaseRunning, getRestore(t, c, "r-hot").Status.Clusters[0].Phase)
	require.Equal(t, api.OperationPhaseRunning, getRestore(t, c, "r-cold").Status.Clusters[0].Phase)

	for i := 0; i < 2; i++ {
		_, err := reconcile("r-hot-again")()
		require.NoError(t, err)
	}
	waiting := getRestore(t, c, "r-hot-again").Status.Clusters[0]
	require.Equal(t, api.OperationPhasePending, waiting.Phase)
	require.Equal(t, api.BackupReasonOperationInProgress, waiting.Reason)
	require.Equal(t, operationInProgressMessage(testCHI, "hot"), waiting.Message)

	finishHanging(sc)
	for _, name := range []string{"r-hot", "r-cold", "r-hot-again"} {
		reconcileUntil(t, reconcile(name), func() bool { return getRestore(t, c, name).Status.Phase.IsTerminal() })
		require.Equal(t, api.OperationPhaseCompleted, getRestore(t, c, name).Status.Phase)
	}
}
