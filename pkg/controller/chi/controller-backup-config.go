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
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/backupconfig"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// backupConfigSyncPeriod is how often the configuration files of the clickhouse-backup sidecars follow the storage
// Secrets they are made of. The operator does not watch Secrets it did not create, as that would cache every Secret
// of the watched namespaces; a changed storage Secret reaches the sidecars within this period plus the time kubelet
// takes to update the files of a mounted Secret, about a minute.
const backupConfigSyncPeriod = time.Minute

// backupConfigSyncTimeout bounds one pass over every installation, so that an API server that does not answer
// does not hold the pass, and the next one, forever
const backupConfigSyncTimeout = 30 * time.Second

// Why the configuration file of a cluster names no storage
var (
	// errBackupStorageSecretNotFound: the storage Secret of the cluster does not exist
	errBackupStorageSecretNotFound = errors.New("does not exist")
	// errBackupStorageConfigNotFound: the storage Secret of the cluster holds no configuration file
	errBackupStorageConfigNotFound = fmt.Errorf("has no key %s", backupconfig.FileName)
)

// backupConfigOutcome is how the configuration file Secret of a cluster has been reconciled
type backupConfigOutcome struct {
	// secret is the Secret reconciled, nil when the configuration file could not be built
	secret *core.Secret
	// storageSecret is the name of the storage Secret of the cluster, "" for none
	storageSecret string
	// noStorage tells why the configuration file names no storage although the cluster has a storage Secret:
	// errBackupStorageSecretNotFound or errBackupStorageConfigNotFound, nil when it does
	noStorage error
	// err is why the Secret was not reconciled: backupconfig.ErrInvalidStorageConfig for storage settings that
	// cannot be read, or a failure to read or write a Secret
	err error
}

// reconcileBackupConfigSecrets reconciles the configuration file Secret of every backup-enabled cluster of the
// normalized installation. cr is the installation the Secrets are made for and owned by, normalized or not.
// Every Secret is reconciled on its own: one cluster whose storage settings cannot be read leaves the others alone.
func (c *Controller) reconcileBackupConfigSecrets(
	ctx context.Context,
	cr *api.ClickHouseInstallation,
	normalized *api.ClickHouseInstallation,
) (map[string]backupConfigOutcome, error) {
	if !normalized.IsBackupEnabled() {
		return nil, nil
	}
	credentials, err := c.getBackupCredentials(ctx, cr)
	if err != nil {
		return nil, err
	}
	creator := buildCreator(cr)
	outcomes := make(map[string]backupConfigOutcome)
	for _, cluster := range normalized.GetSpecT().GetConfiguration().GetClusters() {
		if !cluster.GetBackup().IsEnabled() {
			continue
		}
		outcomes[cluster.GetName()] = c.reconcileBackupConfigSecret(ctx, creator, cr, cluster, credentials)
	}
	return outcomes, nil
}

// reconcileBackupConfigSecret builds the configuration file of the sidecars of the cluster from its storage Secret
// and the credentials, then creates or updates the Secret that holds it. Storage settings that cannot be read leave
// the Secret as it is: the sidecars go on with the storage they have rather than lose it.
func (c *Controller) reconcileBackupConfigSecret(
	ctx context.Context,
	creator *commonCreator.Creator,
	cr *api.ClickHouseInstallation,
	cluster *api.Cluster,
	credentials backupconfig.Credentials,
) backupConfigOutcome {
	outcome := backupConfigOutcome{storageSecret: cluster.GetBackup().GetStorageSecretName()}
	storage, err := c.getBackupStorageConfig(ctx, cr.GetNamespace(), outcome.storageSecret)
	if err != nil {
		outcome.err = err
		return outcome
	}
	outcome.noStorage = storage.missing
	backupConfig, err := backupconfig.Build(storage.config, credentials, backupconfig.Defaults{
		Path:           chop.Config().Backup.StoragePath,
		ObjectDiskPath: chop.Config().Backup.StorageObjectDiskPath,
	})
	if err != nil {
		outcome.err = err
		return outcome
	}
	outcome.secret = creator.CreateBackupConfigSecret(cluster.GetName(), backupConfig)
	outcome.err = c.applyBackupConfigSecret(ctx, outcome.secret)
	return outcome
}

// backupStorageConfig is what a storage Secret holds
type backupStorageConfig struct {
	// config is the storage settings, empty for none
	config []byte
	// missing tells why there are none although the cluster names a storage Secret, nil when there are
	missing error
}

// getBackupStorageConfig returns the storage settings the storage Secret holds, none for no Secret
func (c *Controller) getBackupStorageConfig(ctx context.Context, namespace, name string) (backupStorageConfig, error) {
	if name == "" {
		return backupStorageConfig{}, nil
	}
	secret, err := c.kube.Secret().Get(ctx, &core.Secret{ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: name}})
	switch {
	case apiErrors.IsNotFound(err):
		return backupStorageConfig{missing: errBackupStorageSecretNotFound}, nil
	case err != nil:
		return backupStorageConfig{}, err
	}
	storageConfig, ok := secret.Data[backupconfig.FileName]
	if !ok {
		return backupStorageConfig{missing: errBackupStorageConfigNotFound}, nil
	}
	return backupStorageConfig{config: storageConfig}, nil
}

// getBackupCredentials returns the credentials the backup Secret of the installation holds
func (c *Controller) getBackupCredentials(ctx context.Context, cr api.ICustomResource) (backupconfig.Credentials, error) {
	secret, err := c.kube.Secret().Get(ctx, &core.Secret{ObjectMeta: meta.ObjectMeta{
		Namespace: cr.GetNamespace(),
		Name:      c.namer.Name(interfaces.NameBackupAutoSecret, cr),
	}})
	if err != nil {
		return backupconfig.Credentials{}, err
	}
	credentials := backupconfig.Credentials{
		APIUsername:        string(secret.Data[config.ClickHouseBackupSecretKeyAPIUsername]),
		APIPassword:        string(secret.Data[config.ClickHouseBackupSecretKeyAPIPassword]),
		ClickHouseUsername: string(secret.Data[config.ClickHouseBackupSecretKeyClickHouseUsername]),
		ClickHousePassword: string(secret.Data[config.ClickHouseBackupSecretKeyClickHousePassword]),
	}
	if (credentials.APIUsername == "") || (credentials.APIPassword == "") ||
		(credentials.ClickHouseUsername == "") || (credentials.ClickHousePassword == "") {
		return backupconfig.Credentials{}, fmt.Errorf("backup Secret %s lacks credentials", util.NamespaceNameString(secret))
	}
	return credentials, nil
}

// applyBackupConfigSecret creates the Secret, or updates the data of the one there when it differs. The metadata of
// an existing Secret stays as it is: a pass that does not reconcile the installation must not take labels or owners
// from another state of it than its reconcile does.
func (c *Controller) applyBackupConfigSecret(ctx context.Context, secret *core.Secret) error {
	current, err := c.kube.Secret().Get(ctx, secret)
	switch {
	case apiErrors.IsNotFound(err):
		return c.createSecret(ctx, secret)
	case err != nil:
		return err
	}
	if maps.EqualFunc(current.Data, secret.Data, bytes.Equal) {
		return nil
	}
	current.Data = secret.Data
	if _, err := c.kube.Secret().Update(ctx, current); err != nil {
		return err
	}
	log.V(1).M(secret).F().Info("Updated backup configuration Secret %s", util.NamespacedName(secret))
	return nil
}

// StartBackupConfigSync keeps the configuration files of the clickhouse-backup sidecars of every installation in
// step with their storage Secrets, until ctx is done
func (c *Controller) StartBackupConfigSync(ctx context.Context) {
	go wait.UntilWithContext(ctx, c.syncBackupConfigs, backupConfigSyncPeriod)
}

// syncBackupConfigs reconciles the configuration file Secrets of every installation of the watched namespaces as
// its last completed reconcile normalized it. An installation that has completed none, is being reconciled or
// deleted, or is suspended is left alone: its reconcile creates its Secrets, one in progress has written them from a
// newer spec than the last completed one, and a suspended one is not to change.
func (c *Controller) syncBackupConfigs(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, backupConfigSyncTimeout)
	defer cancel()

	list, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(chop.Config().GetInformerNamespace()).List(ctx, meta.ListOptions{})
	if err != nil {
		log.V(1).Warning("Unable to list installations to sync backup configuration Secrets err: %v", err)
		return
	}
	for i := range list.Items {
		item := &list.Items[i]
		if !chop.Config().IsNamespaceWatched(item.GetNamespace()) || !item.GetDeletionTimestamp().IsZero() || item.Spec.Suspend.Value() {
			continue
		}
		// The normalized state of its last completed reconcile is kept apart from the installation
		obj, err := c.kube.CR().Get(ctx, item.GetNamespace(), item.GetName())
		if err != nil {
			continue
		}
		cr, ok := obj.(*api.ClickHouseInstallation)
		if !ok || (cr == nil) || !cr.HasAncestor() || (cr.EnsureStatus().GetStatus() == api.StatusInProgress) {
			continue
		}
		outcomes, err := c.reconcileBackupConfigSecrets(ctx, cr, cr.GetAncestorT())
		if err != nil {
			log.V(1).M(cr).F().Warning("Unable to sync backup configuration Secrets err: %v", err)
			continue
		}
		for cluster, outcome := range outcomes {
			if outcome.err != nil {
				log.V(1).M(cr).F().Warning("Unable to sync backup configuration Secret of cluster %s err: %v", cluster, outcome.err)
			}
		}
	}
}
