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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	core "k8s.io/api/core/v1"
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
	require.Equal(t, string(credentials[config.ClickHouseBackupSecretKeyClickHousePassword]), file["clickhouse"]["password"])
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

// backupConfigSyncFakeKube serves the Secrets of backupSecretFakeSecret and the installations as stored, with the
// normalized state of their last completed reconcile
type backupConfigSyncFakeKube struct {
	backupSecretFakeKube
	crs map[string]*api.ClickHouseInstallation
}

func (k *backupConfigSyncFakeKube) CR() interfaces.IKubeCR {
	return &backupSecretOwnerFakeCR{crs: k.crs}
}

// newBackupConfigSyncController reconciles an installation with storage Secret bucket "first", which then becomes
// "second", and returns a controller that serves the installation as the operator stores it after mutate
func newBackupConfigSyncController(t *testing.T, mutate func(cr *api.ClickHouseInstallation)) (*Controller, *backupSecretFakeSecret) {
	t.Helper()
	normalized := newBackupSecretCR(t, enabledBackup())
	fake := &backupSecretFakeSecret{secrets: map[string]*core.Secret{
		testStorageSecret: storageSecretWith("s3:\n  bucket: first\n"),
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
