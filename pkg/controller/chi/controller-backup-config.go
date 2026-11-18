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
	"slices"
	"strings"
	"sync"
	"time"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
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

// backupConfigSyncListTimeout bounds the listing of the installations a pass starts with
const backupConfigSyncListTimeout = 30 * time.Second

// backupConfigSyncInstallationTimeout bounds the work of a pass on one installation, so that an API server that does
// not answer does not hold the pass forever, while installations late in a long list get as much time as the first
// ones: a deadline over the whole pass would starve them on every pass alike. A variable for the tests.
var backupConfigSyncInstallationTimeout = 15 * time.Second

// Why the configuration file of a cluster names no storage
var (
	// errBackupStorageSecretNotFound: the storage Secret of the cluster does not exist
	errBackupStorageSecretNotFound = errors.New("does not exist")
	// errBackupStorageConfigNotFound: the storage Secret of the cluster holds no configuration file
	errBackupStorageConfigNotFound = fmt.Errorf("has no key %s", backupconfig.FileName)
	// errBackupStorageConfigEmpty: the storage Secret of the cluster holds an empty configuration file
	errBackupStorageConfigEmpty = fmt.Errorf("has an empty key %s", backupconfig.FileName)
)

// backupConfigOutcome is how the configuration file Secret of a cluster has been reconciled
type backupConfigOutcome struct {
	// secret is the Secret reconciled, or the one there kept as it is when the configuration file could not be
	// built; nil when there is neither
	secret *core.Secret
	// storageSecret is the name of the storage Secret of the cluster, "" for none
	storageSecret string
	// noStorage tells why the storage Secret of the cluster gives no storage settings: errBackupStorageSecretNotFound,
	// errBackupStorageConfigNotFound or errBackupStorageConfigEmpty, nil when it does
	noStorage error
	// keptStorage tells that, the storage Secret giving none, the configuration file kept the storage settings it had
	keptStorage bool
	// err is why the Secret was not reconciled: backupconfig.ErrInvalidStorageConfig for storage settings that
	// cannot be read, or a failure to read or write a Secret
	err error
}

// reconcileBackupConfigSecrets reconciles the configuration file Secret of every backup-enabled cluster of the
// normalized installation. cr is the installation the Secrets are made for and owned by, normalized or not.
// Every Secret is reconciled on its own: one cluster whose storage settings cannot be read leaves the others alone.
//
// keepClickHouseCredentials keeps the ClickHouse credentials of an existing configuration file rather than take the
// ones of the backup Secret, unless the users configuration of ClickHouse already has those. ClickHouse takes the
// password of the backup user from the users configuration, which only a reconcile of the installation writes,
// before it writes the configuration files: a file that took a new password between the reconciles would lock its
// sidecar out of ClickHouse until the next one. A reconcile that wrote the users configuration but failed to write
// the files leaves them to the sync, which then finds the new credentials in the users configuration.
func (c *Controller) reconcileBackupConfigSecrets(
	ctx context.Context,
	cr *api.ClickHouseInstallation,
	normalized *api.ClickHouseInstallation,
	keepClickHouseCredentials bool,
) (map[string]backupConfigOutcome, error) {
	if !normalized.IsBackupEnabled() {
		return nil, nil
	}
	credentials, err := c.getBackupCredentials(ctx, cr)
	if err != nil {
		return nil, err
	}
	// The credentials of a backup user ClickHouse has already are taken at once, see reconcileBackupConfigSecret for
	// the others
	managed := (credentials.ClickHouseUsername != "") && (credentials.ClickHousePassword != "")
	if keepClickHouseCredentials && managed && c.clickHouseHasBackupCredentials(ctx, cr, credentials) {
		keepClickHouseCredentials = false
	}
	creator := buildCreator(cr)
	outcomes := make(map[string]backupConfigOutcome)
	for _, cluster := range normalized.GetSpecT().GetConfiguration().GetClusters() {
		if !cluster.GetBackup().IsEnabled() {
			continue
		}
		outcomes[cluster.GetName()] = c.reconcileBackupConfigSecret(ctx, creator, cr, cluster, credentials, keepClickHouseCredentials)
	}
	return outcomes, nil
}

// reconcileBackupConfigSecret builds the configuration file of the sidecars of the cluster from its storage Secret
// and the credentials, then creates or updates the Secret that holds it. Storage settings that cannot be read leave
// the Secret as it is, and a storage Secret that gives none - gone, say, while it is recreated - leaves the storage
// settings of the file as they are: the sidecars go on with the storage they have rather than lose it.
func (c *Controller) reconcileBackupConfigSecret(
	ctx context.Context,
	creator *commonCreator.Creator,
	cr *api.ClickHouseInstallation,
	cluster *api.Cluster,
	credentials backupconfig.Credentials,
	keepClickHouseCredentials bool,
) backupConfigOutcome {
	outcome := backupConfigOutcome{storageSecret: cluster.GetBackup().GetConfigSecretName()}
	storage, err := c.getBackupStorageConfig(ctx, cr.GetNamespace(), outcome.storageSecret)
	if err != nil {
		outcome.err = err
		return outcome
	}
	outcome.noStorage = storage.missing
	current, err := c.getBackupConfigSecret(ctx, cr.GetNamespace(), c.namer.Name(interfaces.NameBackupConfigSecret, cr, cluster.GetName()))
	if err != nil {
		outcome.err = err
		return outcome
	}
	if current != nil {
		if (storage.missing != nil) && (len(current.Data[backupconfig.FileName]) > 0) {
			// The file keeps its storage settings, and takes the credentials anew: ClickHouse credentials the
			// backup Secret no longer sets are no storage setting to keep
			storage.config = backupconfig.WithoutClickHouseCredentials(current.Data[backupconfig.FileName])
			outcome.keptStorage = true
		}
		if keepClickHouseCredentials {
			switch {
			case len(current.Data[backupconfig.ManagedClickHouseUserKey]) > 0:
				// The file connects as a backup user the operator manages: ClickHouse has that user until a
				// reconcile writes another one, or none
				if username, password, ok := backupconfig.ClickHouseCredentials(current.Data[backupconfig.FileName]); ok {
					credentials.ClickHouseUsername, credentials.ClickHousePassword = username, password
				}
			case (credentials.ClickHouseUsername != "") && (credentials.ClickHousePassword != ""):
				// A backup user ClickHouse does not have yet: the file goes on connecting as it did, as no
				// user of the operator's - with what the storage settings say, or the defaults of clickhouse-backup
				credentials.ClickHouseUsername, credentials.ClickHousePassword = "", ""
			}
		}
	}
	backupConfig, err := backupconfig.Build(storage.config, credentials, backupconfig.Defaults{
		Path:           chop.Config().Backup.StoragePath,
		ObjectDiskPath: chop.Config().Backup.StorageObjectDiskPath,
	})
	if err != nil {
		// The Secret there stays, and stays registered, so that it is not purged as an unknown object
		outcome.secret = current
		outcome.err = err
		return outcome
	}
	outcome.secret = creator.CreateBackupConfigSecret(cluster.GetName(), backupConfig)
	outcome.err = c.applyBackupConfigSecret(ctx, current, outcome.secret)
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
	switch {
	case !ok:
		return backupStorageConfig{missing: errBackupStorageConfigNotFound}, nil
	case len(bytes.TrimSpace(storageConfig)) == 0:
		return backupStorageConfig{missing: errBackupStorageConfigEmpty}, nil
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
	// The ClickHouse credentials are the user's to set, or not
	if (credentials.APIUsername == "") || (credentials.APIPassword == "") {
		return backupconfig.Credentials{}, fmt.Errorf("backup Secret %s lacks credentials", util.NamespaceNameString(secret))
	}
	return credentials, nil
}

// clickHouseHasBackupCredentials tells whether the users configuration of ClickHouse the last reconcile of the
// installation wrote gives the backup user the ClickHouse credentials. A users configuration that cannot be read
// does not.
func (c *Controller) clickHouseHasBackupCredentials(
	ctx context.Context,
	cr *api.ClickHouseInstallation,
	credentials backupconfig.Credentials,
) bool {
	users, err := c.kube.ConfigMap().Get(ctx, cr.GetNamespace(), c.namer.Name(interfaces.NameConfigMapCommonUsers, cr))
	if err != nil {
		return false
	}
	return backupconfig.UsersFileHasPassword(
		[]byte(users.Data[config.ChopGeneratedUsersConfigFilename()]),
		credentials.ClickHouseUsername,
		credentials.ClickHousePassword,
	)
}

// getBackupConfigSecret returns the configuration file Secret of the name, nil when there is none
func (c *Controller) getBackupConfigSecret(ctx context.Context, namespace, name string) (*core.Secret, error) {
	secret, err := c.kube.Secret().Get(ctx, &core.Secret{ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: name}})
	if apiErrors.IsNotFound(err) {
		return nil, nil
	}
	return secret, err
}

// applyBackupConfigSecret creates the Secret when there is none, current being nil, or updates the data of the one
// there when it differs. The metadata of an existing Secret stays as it is: a pass that does not reconcile the
// installation must not take labels or owners from another state of it than its reconcile does.
func (c *Controller) applyBackupConfigSecret(ctx context.Context, current, secret *core.Secret) error {
	if current == nil {
		return c.createSecret(ctx, secret)
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

// backupConfigProblems keeps what is wrong with the configuration file Secret of every backup-enabled cluster as last
// warned about, so that the sync, which runs every minute, warns only when it changes
type backupConfigProblems struct {
	mu       sync.Mutex
	problems map[string]string
}

// backupConfigProblem tells what the outcome warns about: "" when nothing, or a failure to read or write a Secret,
// which may well be gone by the next pass and is logged instead
func backupConfigProblem(outcome backupConfigOutcome) string {
	switch {
	case errors.Is(outcome.err, backupconfig.ErrInvalidStorageConfig):
		return outcome.err.Error()
	case outcome.err != nil:
		return ""
	case outcome.noStorage != nil:
		return outcome.noStorage.Error()
	}
	return ""
}

// backupConfigProblemKey names the cluster of the installation
func backupConfigProblemKey(cr *api.ClickHouseInstallation, cluster string) string {
	return cr.GetNamespace() + "/" + cr.GetName() + "/" + cluster
}

// reported keeps what the outcome warns about as warned about, telling whether that differs from what was before
func (p *backupConfigProblems) reported(cr *api.ClickHouseInstallation, cluster string, outcome backupConfigOutcome) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := backupConfigProblemKey(cr, cluster)
	problem := backupConfigProblem(outcome)
	if p.problems[key] == problem {
		return false
	}
	if p.problems == nil {
		p.problems = make(map[string]string)
	}
	if problem == "" {
		delete(p.problems, key)
	} else {
		p.problems[key] = problem
	}
	return true
}

// retain forgets the clusters of the installations other than those given
func (p *backupConfigProblems) retain(crs []api.ClickHouseInstallation) {
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := make(map[string]bool, len(crs))
	for i := range crs {
		kept[crs[i].GetNamespace()+"/"+crs[i].GetName()] = true
	}
	for key := range p.problems {
		if namespace, rest, _ := strings.Cut(key, "/"); !kept[namespace+"/"+strings.SplitN(rest, "/", 2)[0]] {
			delete(p.problems, key)
		}
	}
}

// StartBackupConfigSync keeps the configuration files of the clickhouse-backup sidecars of every installation in
// step with their storage Secrets, until ctx is done
func (c *Controller) StartBackupConfigSync(ctx context.Context) {
	announcer := a.NewAnnouncer(a.NewEventEmitter(c.kube.Event(), "ClickHouseInstallation", "chop-chi-", componentName), c.kube.CR())
	go wait.UntilWithContext(ctx, func(ctx context.Context) { c.syncBackupConfigs(ctx, announcer) }, backupConfigSyncPeriod)
}

// syncBackupConfigs reconciles the configuration file Secrets of every installation of the watched namespaces as
// its last completed reconcile normalized it. An installation that has completed none, is being reconciled or
// deleted, or is suspended is left alone: its reconcile creates its Secrets, one in progress has written them from a
// newer spec than the last completed one, and a suspended one is not to change. A storage Secret gone or holding
// settings that cannot be read is warned about as the reconcile does, once, when it turns so. The ClickHouse
// credentials of the files stay as the last reconcile wrote them until the users configuration of ClickHouse has the
// ones of the backup Secret, see reconcileBackupConfigSecrets.
//
// Every installation gets a deadline of its own, see backupConfigSyncInstallationTimeout. A pass over many
// installations may then take longer than the sync period, which only makes the next pass start later.
func (c *Controller) syncBackupConfigs(ctx context.Context, announcer a.Announcer) {
	listCtx, cancel := context.WithTimeout(ctx, backupConfigSyncListTimeout)
	list, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(chop.Config().GetInformerNamespace()).List(listCtx, meta.ListOptions{})
	cancel()
	if err != nil {
		log.V(1).Warning("Unable to list installations to sync backup configuration Secrets err: %v", err)
		return
	}
	c.backupConfigProblems.retain(list.Items)
	for i := range list.Items {
		if ctx.Err() != nil {
			return
		}
		item := &list.Items[i]
		if !chop.Config().IsNamespaceWatched(item.GetNamespace()) || !item.GetDeletionTimestamp().IsZero() || item.Spec.Suspend.Value() {
			continue
		}
		c.syncBackupConfig(ctx, announcer, item.GetNamespace(), item.GetName())
	}
}

// syncBackupConfig reconciles the configuration file Secrets of the installation, see syncBackupConfigs
func (c *Controller) syncBackupConfig(ctx context.Context, announcer a.Announcer, namespace, name string) {
	ctx, cancel := context.WithTimeout(ctx, backupConfigSyncInstallationTimeout)
	defer cancel()

	// The normalized state of its last completed reconcile is kept apart from the installation
	obj, err := c.kube.CR().Get(ctx, namespace, name)
	if err != nil {
		return
	}
	cr, ok := obj.(*api.ClickHouseInstallation)
	if !ok || (cr == nil) || !cr.HasAncestor() || (cr.EnsureStatus().GetStatus() == api.StatusInProgress) {
		return
	}
	outcomes, err := c.reconcileBackupConfigSecrets(ctx, cr, cr.GetAncestorT(), true)
	if err != nil {
		log.V(1).M(cr).F().Warning("Unable to sync backup configuration Secrets err: %v", err)
		return
	}
	for _, cluster := range slices.Sorted(maps.Keys(outcomes)) {
		outcome := outcomes[cluster]
		switch {
		case c.backupConfigProblems.reported(cr, cluster, outcome) && (backupConfigProblem(outcome) != ""):
			warnBackupConfigOutcome(announcer, cr, cluster, outcome)
		case (outcome.err != nil) && (backupConfigProblem(outcome) == ""):
			log.V(1).M(cr).F().Warning("Unable to sync backup configuration Secret of cluster %s err: %v", cluster, outcome.err)
		}
	}
}
