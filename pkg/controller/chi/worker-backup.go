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

package chi

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/backupconfig"
	chiCreator "github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// warnBackupWithoutStorage warns about the clusters that turn backup on without the settings of a storage,
// which leaves their backup off: they run without the sidecar, and backups and restores of them are refused -
// with nothing on the installation itself telling why.
func (w *worker) warnBackupWithoutStorage(cr *api.ClickHouseInstallation) {
	var clusters []string
	cr.WalkClusters(func(cluster api.ICluster) error {
		if cluster.GetBackup().LacksStorage() {
			clusters = append(clusters, cluster.GetName())
		}
		return nil
	})
	if len(clusters) == 0 {
		return
	}
	w.a.V(1).M(cr).F().
		WithEvent(cr, a.EventActionReconcile, a.EventReasonBackupNoStorage).
		Warning("Backup stays off on the clusters with backup enabled without a storage: %s - %s",
			strings.Join(clusters, ", "), api.BackupStorageHint)
}

// reconcileBackupConfigSecrets reconciles the configuration file Secret of the sidecars of every backup-enabled
// cluster, registering each so that it is not purged as an unknown object, and warns about the storage Secrets the
// files name no storage by, or cannot be built from. A missing storage Secret leaves the hosts without a storage to
// upload to, and every backup and restore of the cluster fails - with nothing else telling why.
func (w *worker) reconcileBackupConfigSecrets(ctx context.Context, cr *api.ClickHouseInstallation) error {
	outcomes, err := w.c.reconcileBackupConfigSecrets(ctx, cr, cr)
	if err != nil {
		w.a.V(1).M(cr).F().
			WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileFailed).
			Warning("Unable to reconcile backup configuration Secrets err: %v", err)
		return err
	}
	var errs []error
	for _, cluster := range slices.Sorted(maps.Keys(outcomes)) {
		outcome := outcomes[cluster]
		storageSecret := util.NamespaceNameString(&meta.ObjectMeta{Namespace: cr.GetNamespace(), Name: outcome.storageSecret})
		switch {
		case errors.Is(outcome.err, backupconfig.ErrInvalidStorageConfig):
			w.a.V(1).M(cr).F().
				WithEvent(cr, a.EventActionReconcile, a.EventReasonBackupStorageConfigInvalid).
				Warning("Secret %s of the backup storage of cluster %s holds a %s that cannot be read - the sidecars "+
					"keep their configuration until it is fixed: %v", storageSecret, cluster, backupconfig.FileName, outcome.err)
		case outcome.err != nil:
			w.a.V(1).M(cr).F().
				WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileFailed).
				Warning("Unable to reconcile backup configuration Secret of cluster %s err: %v", cluster, outcome.err)
			errs = append(errs, outcome.err)
		case outcome.noStorage != nil:
			w.a.V(1).M(cr).F().
				WithEvent(cr, a.EventActionReconcile, a.EventReasonBackupStorageSecretMissing).
				Warning("Secret %s of the backup storage of cluster %s %v - its backups fail until the Secret holds the "+
					"clickhouse-backup configuration of the storage in key %s, which the sidecars pick up within a few minutes",
					storageSecret, cluster, outcome.noStorage, backupconfig.FileName)
		}
		if outcome.secret == nil {
			continue
		}
		if outcome.err != nil {
			w.task.RegistryFailed().RegisterSecret(outcome.secret.GetObjectMeta())
		} else {
			w.task.RegistryReconciled().RegisterSecret(outcome.secret.GetObjectMeta())
		}
	}
	return errors.Join(errs...)
}

// warnUnpublishedBackupAPI warns about the hosts of backup-enabled clusters whose Service does not publish the
// clickhouse-backup REST API. A Service reachable from outside the cluster does not get the port, so the backup
// Jobs cannot reach these hosts, and every backup and restore of them fails - with nothing else telling why.
func (w *worker) warnUnpublishedBackupAPI(cr *api.ClickHouseInstallation) {
	var hosts []string
	cr.WalkHosts(func(host *api.Host) error {
		if !host.GetCluster().GetBackup().IsEnabled() {
			return nil
		}
		if service := w.task.Creator().CreateService(interfaces.ServiceHost, host).First(); !chiCreator.ServiceHasBackupPort(service) {
			hosts = append(hosts, host.GetName())
		}
		return nil
	})
	if len(hosts) == 0 {
		return
	}
	w.a.V(1).M(cr).F().
		WithEvent(cr, a.EventActionReconcile, a.EventReasonBackupAPIUnpublished).
		Warning("Host Services reachable from outside the cluster do not publish the backup API, "+
			"so backups and restores cannot reach these hosts: %s", strings.Join(hosts, ", "))
}
