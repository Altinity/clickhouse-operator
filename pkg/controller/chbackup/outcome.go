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

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chbackup/metrics"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// The last-outcome gauges live in the operator's memory only. Every reconcile sets them again from
// the status it reads, and an operator restart reconciles every resource, which brings them back, but
// for the clusters that left their installation, or turned their backup off, meanwhile. A controller that
// resolves the clusters of an installation drops the series of those that no longer back up, and one
// that finds the installation gone drops them all. A cluster that turns backup on without a
// storage keeps its series: it backs up no more while it lacks one, which the age of its last backup
// shows. They are variables so that tests can see what is set.
var (
	reportSuccess  = metrics.BackupSucceededAt
	reportFailure  = metrics.BackupFailedAt
	retainClusters = metrics.RetainClusters
)

// retainRequestedClusters drops the last-outcome series of the clusters of an installation, as its last completed
// reconcile left them, that no longer turn backup on.
func retainRequestedClusters(namespace, chiName string, chi *api.ClickHouseInstallation) {
	retainClusters(namespace, chiName, model.BackupRequestedClusters(chi.GetAncestorT()))
}

// reportBackupOutcome sets the last-outcome gauges of the clusters of an installation from the status
// of one of its backups, of those reported tells. A backup is made on a cluster once it completed there,
// verified or not.
func reportBackupOutcome(chb *api.ClickHouseBackup, reported func(cluster string) bool) {
	for _, cluster := range chb.Status.Clusters {
		switch {
		case !reported(cluster.Name):
		case cluster.CompletionTime == nil:
		case cluster.Phase == api.OperationPhaseCompleted:
			reportSuccess(context.Background(), chb.GetNamespace(), chb.TargetInstallation(), cluster.Name, cluster.CompletionTime.Time)
		case cluster.Phase == api.OperationPhaseFailed:
			reportFailure(context.Background(), chb.GetNamespace(), chb.TargetInstallation(), cluster.Name, cluster.CompletionTime.Time)
		}
	}
}

// everyCluster reports every cluster of an installation.
func everyCluster(string) bool {
	return true
}

// reportFinishedBackup sets the last-outcome gauges of the clusters of an installation from the status of
// one of its finished backups, which is read again only after an operator restart, or a resync. By then
// a cluster may have left the installation, or the installation may be gone: the gauges are set of the
// clusters it backs up as its last completed reconcile left it alone, and the series of the others go.
// Nothing is set for an installation that never completed a reconcile, nor for a backup refused before it named
// one, such as one whose schedule was never found.
//
// The installation is read past the cache for every finished backup reconciled, at an operator start and a
// resync above all. Its clusters are not kept by its resourceVersion: the state of its last completed
// reconcile lives in a ConfigMap, whose write is not bound to a change of that version.
func (r *reconciler) reportFinishedBackup(ctx context.Context, chb *api.ClickHouseBackup) error {
	namespace, chiName := chb.GetNamespace(), chb.TargetInstallation()
	if chiName == "" {
		return nil
	}
	chi, err := r.getInstallation(ctx, namespace, chiName)
	if (err != nil) || (chi.GetAncestorT() == nil) {
		return err
	}
	requested := model.BackupRequestedClusters(chi.GetAncestorT())
	retainClusters(namespace, chiName, requested)
	reportBackupOutcome(chb, func(cluster string) bool {
		return slices.Contains(requested, cluster)
	})
	return nil
}

// reportScheduleOutcome sets the last-outcome gauges of the clusters of an installation from the
// status of one of its schedules. A cluster the schedule no longer runs on, whose entry stays for the
// hosts retention left unpruned, is left out: its series went when it left.
func reportScheduleOutcome(chbs *api.ClickHouseBackupSchedule) {
	for _, cluster := range chbs.Status.Clusters {
		if cluster.CronJobName == "" {
			continue
		}
		if succeeded := cluster.LastSuccessfulTime; succeeded != nil {
			reportSuccess(context.Background(), chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, cluster.Name, succeeded.Time)
		}
		if failed := cluster.LastFailureTime; failed != nil {
			reportFailure(context.Background(), chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, cluster.Name, failed.Time)
		}
	}
}
