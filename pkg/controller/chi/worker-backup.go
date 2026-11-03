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
	"strings"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
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

// warnMissingStorageSecrets warns about the storage Secrets of backup-enabled clusters that do not
// exist. The sidecars take such a Secret as optional, so the hosts start without a storage to upload to, and
// every backup and restore of the cluster fails - with nothing else telling why.
func (w *worker) warnMissingStorageSecrets(ctx context.Context, cr *api.ClickHouseInstallation) {
	checked := make(map[string]bool)
	cr.WalkClusters(func(cluster api.ICluster) error {
		backup := cluster.GetBackup()
		name := backup.GetStorageSecretName()
		if !backup.IsEnabled() || (name == "") || checked[name] {
			return nil
		}
		checked[name] = true

		secret := &core.Secret{ObjectMeta: meta.ObjectMeta{Namespace: cr.GetNamespace(), Name: name}}
		_, err := w.c.getSecret(ctx, secret)
		switch {
		case apiErrors.IsNotFound(err):
			w.a.V(1).M(cr).F().
				WithEvent(cr, a.EventActionReconcile, a.EventReasonBackupStorageSecretMissing).
				Warning("Secret %s of backup storage %s of cluster %s does not exist - its backups fail until it is "+
					"created, and the sidecars restart to pick it up a few minutes after",
					util.NamespaceNameString(secret), backup.GetStorageName(), cluster.GetName())
		case err != nil:
			w.a.V(1).M(cr).F().
				Warning("Unable to check Secret %s of backup storage %s of cluster %s err: %v",
					util.NamespaceNameString(secret), backup.GetStorageName(), cluster.GetName(), err)
		}
		return nil
	})
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
