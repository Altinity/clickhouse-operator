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
	"fmt"
	"maps"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/backupconfig"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	chiCreator "github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// warnIncompleteBackupCredentials warns when the backup Secret sets only one of the ClickHouse username and password.
// The operator creates the backup user only when both are set, so there is none then, and the sidecars connect to
// ClickHouse with the one set alone - with nothing else telling why they cannot log in, if they cannot.
func (w *worker) warnIncompleteBackupCredentials(ctx context.Context, cr *api.ClickHouseInstallation) {
	if !cr.IsBackupEnabled() {
		return
	}
	secret, err := w.c.kube.Secret().Get(ctx, &core.Secret{ObjectMeta: meta.ObjectMeta{
		Namespace: cr.GetNamespace(),
		Name:      w.c.namer.Name(interfaces.NameBackupAutoSecret, cr),
	}})
	if err != nil {
		return
	}
	set, unset := config.ClickHouseBackupSecretKeyClickHouseUsername, config.ClickHouseBackupSecretKeyClickHousePassword
	switch username, password := len(secret.Data[set]) > 0, len(secret.Data[unset]) > 0; {
	case username == password:
		return
	case password:
		set, unset = unset, set
	}
	w.a.V(1).M(cr).F().
		WithEvent(cr, a.EventActionReconcile, a.EventReasonBackupCredentialsIncomplete).
		Warning("Backup Secret %s sets %s without %s - the operator creates no backup user, and the sidecars "+
			"connect to ClickHouse with %s alone; set both to have the backup user created, or neither for the "+
			"defaults of clickhouse-backup", util.NamespaceNameString(secret), set, unset, set)
}

// reconcileBackupConfigSecrets reconciles the configuration file Secret of the clickhouse-backup containers of every
// backup-enabled cluster, registering each so that it is not purged as an unknown object. A backup that cannot be
// configured aborts the reconcile, see abortOnInvalidBackupConfig: a backup block out of range, or a storage Secret
// that is missing, holds no configuration file, or one that cannot be read. A cluster backed up without a usable
// storage would fail every backup and restore with nothing on the installation telling why.
func (w *worker) reconcileBackupConfigSecrets(ctx context.Context, cr *api.ClickHouseInstallation) error {
	if problems := backupSpecProblems(cr); len(problems) > 0 {
		return w.abortOnInvalidBackupConfig(cr, problems)
	}
	if problem := w.podModeCredentialsProblem(ctx, cr); problem != "" {
		return w.abortOnInvalidBackupConfig(cr, []string{problem})
	}
	outcomes, err := w.c.reconcileBackupConfigSecrets(ctx, cr, cr, false)
	if err != nil {
		w.a.V(1).M(cr).F().
			WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileFailed).
			Warning("Unable to reconcile backup configuration Secrets err: %v", err)
		return err
	}
	var errs []error
	var invalid []string
	for _, cluster := range slices.Sorted(maps.Keys(outcomes)) {
		outcome := outcomes[cluster]
		w.c.backupConfigProblems.reported(cr, cluster, outcome)
		if problem := backupConfigAbortProblem(cr, cluster, outcome); problem != "" {
			invalid = append(invalid, problem)
		} else {
			warnBackupConfigOutcome(w.a, cr, cluster, outcome)
			if outcome.err != nil {
				errs = append(errs, outcome.err)
			}
		}
		if outcome.secret == nil {
			continue
		}
		// A Secret kept as it is for storage settings that cannot be read is kept on purpose
		if (outcome.err != nil) && !errors.Is(outcome.err, backupconfig.ErrInvalidStorageConfig) {
			w.task.RegistryFailed().RegisterSecret(outcome.secret.GetObjectMeta())
		} else {
			w.task.RegistryReconciled().RegisterSecret(outcome.secret.GetObjectMeta())
		}
	}
	if len(invalid) > 0 {
		return w.abortOnInvalidBackupConfig(cr, invalid)
	}
	return errors.Join(errs...)
}

// backupSpecProblems lists what is wrong with the backup blocks of the installation and of its clusters: a value out
// of range, or a cluster that turns backup on without naming the Secret of its clickhouse-backup configuration -
// backup takes explicit configuration, never defaults. A cluster that turns backup off is never a problem. The
// clusters inherit spec.backup here as the normalizer has them do, so the spec reads alike before and after it.
func backupSpecProblems(cr *api.ClickHouseInstallation) []string {
	var problems []string
	if err := cr.GetSpecT().GetBackup().Validate(); err != nil {
		problems = append(problems, "spec.backup: "+err.Error())
	}
	var lacking []string
	for _, cluster := range cr.GetSpecT().GetConfiguration().GetClusters() {
		if err := cluster.GetBackup().Validate(); err != nil {
			problems = append(problems, "cluster "+cluster.GetName()+": "+err.Error())
		}
		if cluster.GetBackup().DeepCopy().MergeFrom(cr.GetSpecT().GetBackup(), api.MergeTypeFillEmptyValues).LacksStorage() {
			lacking = append(lacking, cluster.GetName())
		}
	}
	if len(lacking) > 0 {
		problems = append(problems, fmt.Sprintf("backup is enabled without a storage on the clusters: %s - %s",
			strings.Join(lacking, ", "), api.BackupStorageHint))
	}
	return problems
}

// podModeClusters lists the backup-enabled clusters of the normalized installation backed up in backup.mode pod
func podModeClusters(cr *api.ClickHouseInstallation) []string {
	var clusters []string
	for _, cluster := range cr.GetSpecT().GetConfiguration().GetClusters() {
		if backup := cluster.GetBackup(); backup.IsEnabled() && backup.IsPodMode() {
			clusters = append(clusters, cluster.GetName())
		}
	}
	return clusters
}

// podModeCredentialsProblem tells why the clusters of the installation backed up in backup.mode pod cannot be, ""
// when they can: the backup Pods connect to ClickHouse over the network, through the host Service of their replica,
// which only the backup user the operator manages may - and the operator manages one only while the backup Secret
// sets both its ClickHouse username and password. A backup Secret that cannot be read is left to the reconcile of
// the configuration files, which reports it.
func (w *worker) podModeCredentialsProblem(ctx context.Context, cr *api.ClickHouseInstallation) string {
	clusters := podModeClusters(cr)
	if len(clusters) == 0 {
		return ""
	}
	credentials, err := w.c.getBackupCredentials(ctx, cr)
	if (err != nil) || ((credentials.ClickHouseUsername != "") && (credentials.ClickHousePassword != "")) {
		return ""
	}
	secret := util.NamespaceNameString(&meta.ObjectMeta{Namespace: cr.GetNamespace(), Name: w.c.namer.Name(interfaces.NameBackupAutoSecret, cr)})
	return fmt.Sprintf("the backup Pods of the clusters in backup.mode pod (%s) connect to ClickHouse over the network as "+
		"the backup user, which needs credentials: set keys %s and %s of Secret %s", strings.Join(clusters, ", "),
		config.ClickHouseBackupSecretKeyClickHouseUsername, config.ClickHouseBackupSecretKeyClickHousePassword, secret)
}

// backupConfigAbortProblem tells why the configuration file of the cluster cannot be made from its storage Secret,
// "" when it can: the Secret is missing, holds no configuration file or an empty one, holds one that cannot be read,
// or cannot be read itself
func backupConfigAbortProblem(cr *api.ClickHouseInstallation, cluster string, outcome backupConfigOutcome) string {
	storageSecret := util.NamespaceNameString(&meta.ObjectMeta{Namespace: cr.GetNamespace(), Name: outcome.storageSecret})
	switch {
	case errors.Is(outcome.err, backupconfig.ErrInvalidStorageConfig):
		return fmt.Sprintf("cluster %s: Secret %s holds a %s that cannot be read: %v", cluster, storageSecret, backupconfig.FileName, outcome.err)
	case errors.Is(outcome.err, errBackupStorageSecretUnreadable):
		return fmt.Sprintf("cluster %s: Secret %s %v", cluster, storageSecret, outcome.err)
	case outcome.noStorage != nil:
		return fmt.Sprintf("cluster %s: Secret %s %v - it has to hold the clickhouse-backup configuration in key %s",
			cluster, storageSecret, outcome.noStorage, backupconfig.FileName)
	}
	return ""
}

// abortOnInvalidBackupConfig marks the reconcile of the installation Aborted for the reason
// api.StatusReasonBackupConfigInvalid and warns about it on the installation. The objects of the installation stay
// as they are. The sync of the configuration files reconciles the installation anew once its storage Secrets can be
// used, see syncBackupConfigs; a backup block out of range takes a spec edit.
func (w *worker) abortOnInvalidBackupConfig(cr *api.ClickHouseInstallation, problems []string) error {
	message := strings.Join(problems, "; ")
	cr.EnsureStatus().ReconcileAbortWithReason(api.StatusReasonBackupConfigInvalid, message)
	w.a.V(1).M(cr).F().
		WithEvent(cr, a.EventActionReconcile, a.EventReasonBackupStorageConfigInvalid).
		Warning("Reconcile aborted, the backup cannot be configured: %s", message)
	return fmt.Errorf("%w: %s: %s", common.ErrCRUDAbort, api.StatusReasonBackupConfigInvalid, message)
}

// warnBackupConfigOutcome warns about the configuration file Secret of the cluster when it names no storage although
// the cluster has a storage Secret, or could not be reconciled
func warnBackupConfigOutcome(announcer a.Announcer, cr *api.ClickHouseInstallation, cluster string, outcome backupConfigOutcome) {
	storageSecret := util.NamespaceNameString(&meta.ObjectMeta{Namespace: cr.GetNamespace(), Name: outcome.storageSecret})
	switch {
	case errors.Is(outcome.err, backupconfig.ErrInvalidStorageConfig):
		announcer.V(1).M(cr).F().
			WithEvent(cr, a.EventActionReconcile, a.EventReasonBackupStorageConfigInvalid).
			Warning("Secret %s of the backup storage of cluster %s holds a %s that cannot be read - the sidecars "+
				"keep their configuration until it is fixed: %v", storageSecret, cluster, backupconfig.FileName, outcome.err)
	case outcome.err != nil:
		announcer.V(1).M(cr).F().
			WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileFailed).
			Warning("Unable to reconcile backup configuration Secret of cluster %s err: %v", cluster, outcome.err)
	case (outcome.noStorage != nil) && outcome.keptStorage:
		announcer.V(1).M(cr).F().
			WithEvent(cr, a.EventActionReconcile, a.EventReasonBackupStorageSecretMissing).
			Warning("Secret %s of the backup storage of cluster %s %v - the sidecars keep the storage settings they "+
				"have until the Secret holds the clickhouse-backup configuration of the storage in key %s again",
				storageSecret, cluster, outcome.noStorage, backupconfig.FileName)
	case outcome.noStorage != nil:
		announcer.V(1).M(cr).F().
			WithEvent(cr, a.EventActionReconcile, a.EventReasonBackupStorageSecretMissing).
			Warning("Secret %s of the backup storage of cluster %s %v - its backups fail until the Secret holds the "+
				"clickhouse-backup configuration of the storage in key %s, which the sidecars pick up within a few minutes",
				storageSecret, cluster, outcome.noStorage, backupconfig.FileName)
	}
}

// warnUnpublishedBackupAPI warns about the hosts of backup-enabled clusters whose Service does not publish the
// clickhouse-backup REST API. A Service reachable from outside the cluster does not get the port, so the backup
// Jobs cannot reach these hosts, and every backup and restore of them fails - with nothing else telling why.
func (w *worker) warnUnpublishedBackupAPI(cr *api.ClickHouseInstallation) {
	var hosts []string
	cr.WalkHosts(func(host *api.Host) error {
		if !chiCreator.HostServesBackupAPI(host) {
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
