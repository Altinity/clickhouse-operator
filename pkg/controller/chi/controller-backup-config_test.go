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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	chopFake "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned/fake"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/backupconfig"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// configSecretName is the configuration file Secret of cluster "c" of the installation newBackupSecretCR makes
const configSecretName = "chi-backup-c-backup-config"

// storageSecretWith is the storage Secret enabledBackup names, holding the storage settings given
func storageSecretWith(storageConfig string) *core.Secret {
	return &core.Secret{
		ObjectMeta: meta.ObjectMeta{Name: testStorageSecret},
		Data:       map[string][]byte{backupconfig.FileName: []byte(storageConfig)},
	}
}

// configFile reads the configuration file the Secret holds
func configFile(t *testing.T, secret *core.Secret) map[string]map[string]any {
	t.Helper()
	file := map[string]map[string]any{}
	require.NoError(t, yaml.Unmarshal(secret.Data[backupconfig.FileName], &file))
	return file
}

// The configuration file of the sidecars of a cluster is built from its storage Secret and the backup Secret, and
// kept in a Secret owned by the installation, registered as reconciled so that it is not purged. The Secret holds the
// settings the path check of the backup Jobs reads as well, under their env var names.
func TestReconcileBackupConfigSecret(t *testing.T) {
	cr := newBackupSecretCR(t, enabledBackup())
	cr.UID = "chi-uid"
	fake := &backupSecretFakeSecret{secrets: map[string]*core.Secret{
		testStorageSecret: storageSecretWith("general:\n  remote_storage: s3\ns3:\n  bucket: backups\n  secret_key: key\n"),
	}}
	w := newBackupSecretWorker(fake, cr)
	require.NoError(t, w.reconcileCRAuxObjectsPreliminaryDomain(context.Background(), cr))

	secret, ok := fake.secrets[configSecretName]
	require.True(t, ok, "the configuration file Secret is created")
	require.True(t, w.task.RegistryReconciled().HasSecret(&secret.ObjectMeta))
	require.Equal(t, cr.GetName(), secret.OwnerReferences[0].Name)
	require.Equal(t, cr.UID, secret.OwnerReferences[0].UID)

	file := configFile(t, secret)
	require.Equal(t, "backups", file["s3"]["bucket"])
	require.Equal(t, "key", file["s3"]["secret_key"])
	credentials := fake.secrets[cr.GetName()+"-auto-backup-secret"].Data
	require.Equal(t, string(credentials[config.ClickHouseBackupSecretKeyAPIPassword]), file["api"]["password"])
	require.Nil(t, file["clickhouse"], "no ClickHouse credentials in the Secret, none in the file: clickhouse-backup's own defaults")
	require.Equal(t, "s3", string(secret.Data["REMOTE_STORAGE"]))
	require.Equal(t, "backups", string(secret.Data["S3_BUCKET"]))
	require.NotContains(t, secret.Data, "S3_SECRET_KEY", "no credential is a setting of the path check")
	fingerprint := string(secret.Data[backupconfig.APIFingerprintFileName])
	require.Len(t, fingerprint, 64, "the sidecar restarts its server when the credentials of its REST API change")
	require.NotContains(t, fingerprint, string(credentials[config.ClickHouseBackupSecretKeyAPIPassword]))
}

// A changed storage Secret changes the configuration file, and nothing else does: an unchanged one leaves the Secret
// as it is. Storage settings that cannot be read leave it as it is too, so that the sidecars go on with the storage
// they have.
func TestReconcileBackupConfigSecretFollowsTheStorageSecret(t *testing.T) {
	cr := newBackupSecretCR(t, enabledBackup())
	fake := &backupSecretFakeSecret{secrets: map[string]*core.Secret{
		testStorageSecret: storageSecretWith("s3:\n  bucket: first\n"),
	}}
	reconcile := func() {
		t.Helper()
		w := newBackupSecretWorker(fake, cr)
		w.a = a.NewAnnouncer(&recordingEventEmitter{}, nil)
		require.NoError(t, w.reconcileCRAuxObjectsPreliminaryDomain(context.Background(), cr))
	}

	reconcile()
	require.Equal(t, "first", configFile(t, fake.secrets[configSecretName])["s3"]["bucket"])
	updates := fake.updates

	reconcile()
	require.Equal(t, updates, fake.updates, "an unchanged storage Secret leaves the configuration file alone")

	fake.secrets[testStorageSecret] = storageSecretWith("s3:\n  bucket: second\n")
	fake.secrets[configSecretName].Labels = map[string]string{"kept": "yes"}
	reconcile()
	require.Equal(t, updates+1, fake.updates)
	require.Equal(t, "second", configFile(t, fake.secrets[configSecretName])["s3"]["bucket"])
	require.Equal(t, "yes", fake.secrets[configSecretName].Labels["kept"], "the metadata of the Secret stays as it is")

	fake.secrets[testStorageSecret] = storageSecretWith("s3: [")
	reconcile()
	require.Equal(t, updates+1, fake.updates, "storage settings that cannot be read change nothing")
	require.Equal(t, "second", configFile(t, fake.secrets[configSecretName])["s3"]["bucket"])
}

// backupConfigSyncFakeKube serves the Secrets of backupSecretFakeSecret, the ConfigMaps of configMaps and the
// installations as stored, with the normalized state of their last completed reconcile
type backupConfigSyncFakeKube struct {
	backupSecretFakeKube
	crs        map[string]*api.ClickHouseInstallation
	configMaps map[string]*core.ConfigMap
}

func (k *backupConfigSyncFakeKube) ConfigMap() interfaces.IKubeConfigMap {
	return &backupConfigSyncFakeConfigMap{configMaps: k.configMaps}
}

// backupConfigSyncFakeConfigMap serves the ConfigMaps it holds, read only
type backupConfigSyncFakeConfigMap struct {
	interfaces.IKubeConfigMap
	configMaps map[string]*core.ConfigMap
}

func (f *backupConfigSyncFakeConfigMap) Get(ctx context.Context, namespace, name string) (*core.ConfigMap, error) {
	if cm, ok := f.configMaps[name]; ok {
		return cm.DeepCopy(), nil
	}
	return nil, apiErrors.NewNotFound(core.Resource("configmaps"), name)
}

// usersConfigMapWith is the users ConfigMap of the installation newBackupSecretCR makes, giving the user the password
func usersConfigMapWith(username, password string) *core.ConfigMap {
	sum := sha256.Sum256([]byte(password))
	users := api.NewSettings()
	users.Set(username+"/password_sha256_hex", api.NewSettingScalar(hex.EncodeToString(sum[:])))
	return &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "chi-chi-backup-common-usersd"},
		Data:       map[string]string{config.ChopGeneratedUsersConfigFilename(): users.ClickHouseConfig("users")},
	}
}

func (k *backupConfigSyncFakeKube) CR() interfaces.IKubeCR {
	return &backupSecretOwnerFakeCR{crs: k.crs}
}

// testBackupUsername is the ClickHouse username the backup Secrets of the tests set: the operator has none of its own
const testBackupUsername = "backup_user"

// testBackupPassword is the ClickHouse password the user sets in the backup Secret of newBackupConfigSyncController
const testBackupPassword = "set-by-user"

// newBackupConfigSyncController reconciles an installation with storage Secret bucket "first", which then becomes
// "second", and returns a controller that serves the installation as the operator stores it after mutate. The user
// has set the ClickHouse credentials of the backup Secret.
func newBackupConfigSyncController(t *testing.T, mutate func(cr *api.ClickHouseInstallation)) (*Controller, *backupSecretFakeSecret) {
	t.Helper()
	normalized := newBackupSecretCR(t, enabledBackup())
	fake := &backupSecretFakeSecret{secrets: map[string]*core.Secret{
		testStorageSecret: storageSecretWith("s3:\n  bucket: first\n"),
		"chi-backup-auto-backup-secret": {
			ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "chi-backup-auto-backup-secret"},
			Data: map[string][]byte{
				config.ClickHouseBackupSecretKeyClickHouseUsername: []byte(testBackupUsername),
				config.ClickHouseBackupSecretKeyClickHousePassword: []byte(testBackupPassword),
			},
		},
	}}
	w := newBackupSecretWorker(fake, normalized)
	require.NoError(t, w.reconcileCRAuxObjectsPreliminaryDomain(context.Background(), normalized))
	fake.secrets[testStorageSecret] = storageSecretWith("s3:\n  bucket: second\n")

	stored := &api.ClickHouseInstallation{ObjectMeta: *normalized.ObjectMeta.DeepCopy()}
	stored.Spec.Backup = enabledBackup()
	// As the operator reads it back from where it keeps it, without the runtime of the reconcile
	data, err := json.Marshal(&api.ClickHouseInstallation{ObjectMeta: normalized.ObjectMeta, Spec: normalized.Spec})
	require.NoError(t, err)
	ancestor := &api.ClickHouseInstallation{}
	require.NoError(t, json.Unmarshal(data, ancestor))
	stored.EnsureStatus().NormalizedCRCompleted = ancestor
	if mutate != nil {
		mutate(stored)
	}
	return &Controller{
		chopClient: chopFake.NewSimpleClientset(stored),
		kube: &backupConfigSyncFakeKube{
			backupSecretFakeKube: backupSecretFakeKube{secret: fake},
			crs:                  map[string]*api.ClickHouseInstallation{stored.GetName(): stored},
			configMaps:           map[string]*core.ConfigMap{},
		},
		namer: managers.NewNameManager(managers.NameManagerTypeClickHouse),
	}, fake
}

// Between the reconciles of an installation, the configuration files follow its storage Secrets as its last completed
// reconcile normalized it. An installation that has completed none, is being reconciled, or is suspended is left
// alone.
func TestSyncBackupConfigs(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(cr *api.ClickHouseInstallation)
		want   string
	}{
		"completed":  {want: "second"},
		"suspended":  {mutate: func(cr *api.ClickHouseInstallation) { cr.Spec.Suspend = types.NewStringBool(true) }, want: "first"},
		"reconciled": {mutate: func(cr *api.ClickHouseInstallation) { cr.EnsureStatus().Status = api.StatusInProgress }, want: "first"},
		"none":       {mutate: func(cr *api.ClickHouseInstallation) { cr.Status.NormalizedCRCompleted = nil }, want: "first"},
		"backup off": {
			mutate: func(cr *api.ClickHouseInstallation) {
				cr.Status.NormalizedCRCompleted.Spec.Backup = nil
				cr.Status.NormalizedCRCompleted.Spec.Configuration.Clusters[0].Backup = nil
			},
			want: "first",
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, fake := newBackupConfigSyncController(t, tc.mutate)
			c.syncBackupConfigs(context.Background(), a.NewAnnouncer(&recordingEventEmitter{}, nil))
			require.Equal(t, tc.want, configFile(t, fake.secrets[configSecretName])["s3"]["bucket"])
		})
	}
}

// The sync warns about a storage Secret gone, or holding settings that cannot be read, as the reconcile does - once
// when it turns so, not on every pass
func TestSyncBackupConfigsWarnsOnChange(t *testing.T) {
	c, fake := newBackupConfigSyncController(t, nil)
	events := &recordingEventEmitter{}
	sync := func() []string {
		t.Helper()
		events.events = nil
		c.syncBackupConfigs(context.Background(), a.NewAnnouncer(events, nil))
		var reasons []string
		for _, event := range events.events {
			reasons = append(reasons, event.reason)
		}
		return reasons
	}

	require.Empty(t, sync())
	delete(fake.secrets, testStorageSecret)
	require.Equal(t, []string{a.EventReasonBackupStorageSecretMissing}, sync())
	require.Empty(t, sync(), "a storage Secret still gone is warned about once")
	fake.secrets[testStorageSecret] = storageSecretWith("s3: [")
	require.Equal(t, []string{a.EventReasonBackupStorageConfigInvalid}, sync())
	require.Empty(t, sync())
	fake.secrets[testStorageSecret] = storageSecretWith("s3:\n  bucket: third\n")
	require.Empty(t, sync())
	require.Equal(t, "third", configFile(t, fake.secrets[configSecretName])["s3"]["bucket"])
	delete(fake.secrets, testStorageSecret)
	require.Equal(t, []string{a.EventReasonBackupStorageSecretMissing}, sync(), "a storage Secret gone again is warned about again")
}

// The sync follows a change of the REST API credentials of the backup Secret, but keeps the ClickHouse credentials
// the last reconcile wrote: ClickHouse takes the password of the backup user from the users configuration, which
// only a reconcile writes, so a sidecar that took a new one between the reconciles would be locked out. The next
// reconcile writes the new ones.
func TestSyncBackupConfigsKeepsTheClickHouseCredentials(t *testing.T) {
	c, fake := newBackupConfigSyncController(t, nil)
	backupSecret := fake.secrets["chi-backup-auto-backup-secret"]
	require.NotNil(t, backupSecret)
	before := configFile(t, fake.secrets[configSecretName])
	backupSecret.Data[config.ClickHouseBackupSecretKeyAPIPassword] = []byte("new-api-password")
	backupSecret.Data[config.ClickHouseBackupSecretKeyClickHousePassword] = []byte("new-clickhouse-password")

	c.syncBackupConfigs(context.Background(), a.NewAnnouncer(&recordingEventEmitter{}, nil))
	file := configFile(t, fake.secrets[configSecretName])
	require.Equal(t, "second", file["s3"]["bucket"])
	require.Equal(t, "new-api-password", file["api"]["password"])
	require.Equal(t, before["clickhouse"]["password"], file["clickhouse"]["password"])
	require.Equal(t, before["clickhouse"]["username"], file["clickhouse"]["username"])

	cr := newBackupSecretCR(t, enabledBackup())
	w := newBackupSecretWorker(fake, cr)
	require.NoError(t, w.reconcileCRAuxObjectsPreliminaryDomain(context.Background(), cr))
	require.Equal(t, "new-clickhouse-password", configFile(t, fake.secrets[configSecretName])["clickhouse"]["password"])
}

// A reconcile that wrote the users configuration with new ClickHouse credentials, then failed to write the
// configuration files, leaves them to the sync: once the users configuration has the credentials of the backup
// Secret, the sync takes them, whatever the files hold. Credentials of no user the operator manages - a username or
// a password not set - are taken as they are.
func TestSyncBackupConfigsTakesTheClickHouseCredentialsOfTheUsersConfiguration(t *testing.T) {
	c, fake := newBackupConfigSyncController(t, nil)
	kube := c.kube.(*backupConfigSyncFakeKube)
	backupSecret := fake.secrets["chi-backup-auto-backup-secret"]
	before := configFile(t, fake.secrets[configSecretName])
	sync := func() map[string]map[string]any {
		t.Helper()
		c.syncBackupConfigs(context.Background(), a.NewAnnouncer(&recordingEventEmitter{}, nil))
		return configFile(t, fake.secrets[configSecretName])
	}

	backupSecret.Data[config.ClickHouseBackupSecretKeyClickHouseUsername] = []byte("backup_rotated")
	backupSecret.Data[config.ClickHouseBackupSecretKeyClickHousePassword] = []byte("rotated")
	kube.configMaps["chi-chi-backup-common-usersd"] = usersConfigMapWith(testBackupUsername, "rotated")
	require.Equal(t, before["clickhouse"], sync()["clickhouse"], "another user with the password is no match")

	kube.configMaps["chi-chi-backup-common-usersd"] = usersConfigMapWith("backup_rotated", "rotated")
	file := sync()
	require.Equal(t, "backup_rotated", file["clickhouse"]["username"])
	require.Equal(t, "rotated", file["clickhouse"]["password"])

	require.Equal(t, "backup_rotated", string(fake.secrets[configSecretName].Data[backupconfig.ManagedClickHouseUserKey]))

	// Credentials removed: the backup user stays in ClickHouse until a reconcile removes it, so the sync keeps
	// connecting as it
	delete(backupSecret.Data, config.ClickHouseBackupSecretKeyClickHouseUsername)
	delete(backupSecret.Data, config.ClickHouseBackupSecretKeyClickHousePassword)
	file = sync()
	require.Equal(t, "backup_rotated", file["clickhouse"]["username"])
	require.Equal(t, "rotated", file["clickhouse"]["password"])

	// The reconcile removes the user and the credentials alike; the file names no user of the operator's then
	reconcile := func() {
		t.Helper()
		cr := newBackupSecretCR(t, enabledBackup())
		require.NoError(t, newBackupSecretWorker(fake, cr).reconcileCRAuxObjectsPreliminaryDomain(context.Background(), cr))
	}
	reconcile()
	require.Nil(t, configFile(t, fake.secrets[configSecretName])["clickhouse"])
	require.NotContains(t, fake.secrets[configSecretName].Data, backupconfig.ManagedClickHouseUserKey)
	delete(kube.configMaps, "chi-chi-backup-common-usersd")

	// Credentials set anew wait for the users configuration: the file goes on connecting with none until then
	backupSecret.Data[config.ClickHouseBackupSecretKeyClickHouseUsername] = []byte("backup_again")
	backupSecret.Data[config.ClickHouseBackupSecretKeyClickHousePassword] = []byte("again")
	require.Nil(t, sync()["clickhouse"])
	kube.configMaps["chi-chi-backup-common-usersd"] = usersConfigMapWith("backup_again", "again")
	file = sync()
	require.Equal(t, "backup_again", file["clickhouse"]["username"])
	require.Equal(t, "again", file["clickhouse"]["password"])
}

// Credentials of no user of the operator's - one of the two only - are taken by the sync as they are: there is no
// backup user for ClickHouse to catch up with
func TestSyncBackupConfigsTakesIncompleteCredentials(t *testing.T) {
	c, fake := newBackupConfigSyncController(t, nil)
	backupSecret := fake.secrets["chi-backup-auto-backup-secret"]
	delete(backupSecret.Data, config.ClickHouseBackupSecretKeyClickHouseUsername)
	delete(backupSecret.Data, config.ClickHouseBackupSecretKeyClickHousePassword)
	cr := newBackupSecretCR(t, enabledBackup())
	require.NoError(t, newBackupSecretWorker(fake, cr).reconcileCRAuxObjectsPreliminaryDomain(context.Background(), cr))

	backupSecret.Data[config.ClickHouseBackupSecretKeyClickHousePassword] = []byte("only-password")
	c.syncBackupConfigs(context.Background(), a.NewAnnouncer(&recordingEventEmitter{}, nil))
	file := configFile(t, fake.secrets[configSecretName])
	require.Equal(t, "only-password", file["clickhouse"]["password"])
	require.NotContains(t, file["clickhouse"], "username")
}

// A storage Secret that gives no storage settings - gone while it is recreated, without the key, or with an empty
// one - leaves the configuration file with the storage settings it has, still following the credentials
func TestSyncBackupConfigsKeepsTheStorageOfAStorageSecretGone(t *testing.T) {
	for name, gone := range map[string]func(secrets map[string]*core.Secret){
		"gone":          func(secrets map[string]*core.Secret) { delete(secrets, testStorageSecret) },
		"without a key": func(secrets map[string]*core.Secret) { secrets[testStorageSecret].Data = map[string][]byte{} },
		"empty":         func(secrets map[string]*core.Secret) { secrets[testStorageSecret] = storageSecretWith("\n") },
	} {
		t.Run(name, func(t *testing.T) {
			c, fake := newBackupConfigSyncController(t, nil)
			gone(fake.secrets)
			fake.secrets["chi-backup-auto-backup-secret"].Data[config.ClickHouseBackupSecretKeyAPIPassword] = []byte("new-api-password")
			c.syncBackupConfigs(context.Background(), a.NewAnnouncer(&recordingEventEmitter{}, nil))
			file := configFile(t, fake.secrets[configSecretName])
			require.Equal(t, "first", file["s3"]["bucket"])
			require.Equal(t, "new-api-password", file["api"]["password"])

			fake.secrets[testStorageSecret] = storageSecretWith("s3:\n  bucket: third\n")
			c.syncBackupConfigs(context.Background(), a.NewAnnouncer(&recordingEventEmitter{}, nil))
			require.Equal(t, "third", configFile(t, fake.secrets[configSecretName])["s3"]["bucket"])
		})
	}
}

// backupConfigSyncSlowCR does not answer for the installations it holds slow until the context of the call is done
type backupConfigSyncSlowCR struct {
	backupSecretOwnerFakeCR
	slow map[string]bool
}

func (f *backupConfigSyncSlowCR) Get(ctx context.Context, namespace, name string) (api.ICustomResource, error) {
	if f.slow[name] {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.backupSecretOwnerFakeCR.Get(ctx, namespace, name)
}

type backupConfigSyncSlowKube struct {
	backupConfigSyncFakeKube
	slow map[string]bool
}

func (k *backupConfigSyncSlowKube) CR() interfaces.IKubeCR {
	return &backupConfigSyncSlowCR{backupSecretOwnerFakeCR: backupSecretOwnerFakeCR{crs: k.crs}, slow: k.slow}
}

// Installations that the API server is slow to answer for use up their own deadlines, not the others': the
// installations listed after them are synced on every pass all the same
func TestSyncBackupConfigsBoundsEveryInstallation(t *testing.T) {
	timeout := backupConfigSyncInstallationTimeout
	backupConfigSyncInstallationTimeout = 20 * time.Millisecond
	defer func() { backupConfigSyncInstallationTimeout = timeout }()

	c, fake := newBackupConfigSyncController(t, nil)
	kube := c.kube.(*backupConfigSyncFakeKube)
	slow := map[string]bool{}
	client := c.chopClient.(*chopFake.Clientset)
	installations := client.ClickhouseV1().ClickHouseInstallations("ns")
	stored, err := installations.Get(context.Background(), "chi-backup", meta.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, installations.Delete(context.Background(), stored.GetName(), meta.DeleteOptions{}))
	for i := 0; i < 20; i++ {
		cr := &api.ClickHouseInstallation{ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: fmt.Sprintf("a-slow-%02d", i)}}
		_, err := installations.Create(context.Background(), cr, meta.CreateOptions{})
		require.NoError(t, err)
		slow[cr.GetName()] = true
	}
	stored.ResourceVersion = ""
	_, err = installations.Create(context.Background(), stored, meta.CreateOptions{})
	require.NoError(t, err)
	list, err := installations.List(context.Background(), meta.ListOptions{})
	require.NoError(t, err)
	require.Equal(t, stored.GetName(), list.Items[len(list.Items)-1].GetName(), "the slow installations are listed first")
	c.kube = &backupConfigSyncSlowKube{backupConfigSyncFakeKube: *kube, slow: slow}

	started := time.Now()
	c.syncBackupConfigs(context.Background(), a.NewAnnouncer(&recordingEventEmitter{}, nil))
	require.Equal(t, "second", configFile(t, fake.secrets[configSecretName])["s3"]["bucket"])
	require.Less(t, time.Since(started), 5*time.Second)
}

// A file that keeps its storage settings while the storage Secret is gone still follows the ClickHouse credentials:
// credentials removed from the backup Secret do not stay in it as if they were storage settings
func TestReconcileBackupConfigKeptStorageDropsRemovedCredentials(t *testing.T) {
	_, fake := newBackupConfigSyncController(t, nil)
	require.Equal(t, testBackupUsername, configFile(t, fake.secrets[configSecretName])["clickhouse"]["username"])
	delete(fake.secrets, testStorageSecret)
	backupSecret := fake.secrets["chi-backup-auto-backup-secret"]
	delete(backupSecret.Data, config.ClickHouseBackupSecretKeyClickHouseUsername)
	delete(backupSecret.Data, config.ClickHouseBackupSecretKeyClickHousePassword)

	cr := newBackupSecretCR(t, enabledBackup())
	require.NoError(t, newBackupSecretWorker(fake, cr).reconcileCRAuxObjectsPreliminaryDomain(context.Background(), cr))
	file := configFile(t, fake.secrets[configSecretName])
	require.Equal(t, "first", file["s3"]["bucket"], "the storage settings are kept")
	require.Nil(t, file["clickhouse"], "the credentials removed are not")
}
