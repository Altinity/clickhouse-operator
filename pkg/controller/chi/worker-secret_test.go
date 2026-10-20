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
	"errors"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	chiNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

// backupSecretFakeSecret keeps Secrets the way the API server does - StringData lands in Data - and panics
// on delete: the backup Secret's values must survive every reconcile, so nothing may ever delete it.
type backupSecretFakeSecret struct {
	secrets map[string]*core.Secret
	getErr  error
	gets    int
	creates int
	updates int
}

func (f *backupSecretFakeSecret) Get(ctx context.Context, params ...any) (*core.Secret, error) {
	f.gets++
	name := params[0].(*core.Secret).Name
	if f.getErr != nil {
		return nil, f.getErr
	}
	secret, ok := f.secrets[name]
	if !ok {
		return nil, apiErrors.NewNotFound(core.Resource("secrets"), name)
	}
	return secret.DeepCopy(), nil
}

func (f *backupSecretFakeSecret) Create(ctx context.Context, secret *core.Secret) (*core.Secret, error) {
	f.creates++
	stored := secret.DeepCopy()
	stored.Data = make(map[string][]byte)
	for key, value := range stored.StringData {
		stored.Data[key] = []byte(value)
	}
	stored.StringData = nil
	f.secrets[secret.Name] = stored
	return stored.DeepCopy(), nil
}

func (f *backupSecretFakeSecret) Update(ctx context.Context, secret *core.Secret) (*core.Secret, error) {
	f.updates++
	f.secrets[secret.Name] = secret.DeepCopy()
	return secret.DeepCopy(), nil
}

func (f *backupSecretFakeSecret) Delete(ctx context.Context, namespace, name string) error {
	panic("the backup Secret must never be deleted by a reconcile")
}

func (f *backupSecretFakeSecret) List(ctx context.Context, namespace string, opts meta.ListOptions) ([]core.Secret, error) {
	return nil, nil
}

// backupSecretFakeKube exposes Secret() only. IKube is embedded as a nil interface, so reaching any
// other accessor panics.
type backupSecretFakeKube struct {
	interfaces.IKube
	secret interfaces.IKubeSecret
}

func (k *backupSecretFakeKube) Secret() interfaces.IKubeSecret { return k.secret }

// testStorageSecret is the storage Secret enabledBackup names, without which backup stays off
const testStorageSecret = "s3-credentials"

// enabledBackup turns backup on with a storage Secret
func enabledBackup() *api.ClusterBackup {
	return &api.ClusterBackup{
		Enabled:  types.NewStringBool(true),
		Storages: []api.BackupStorage{{Name: "main", SecretRef: &core.LocalObjectReference{Name: testStorageSecret}}},
	}
}

func newBackupSecretCR(t *testing.T, backup *api.ClusterBackup) *api.ClickHouseInstallation {
	t.Helper()
	cr := &api.ClickHouseInstallation{}
	cr.Name = "chi-backup"
	cr.Namespace = "ns"
	cr.Spec.Backup = backup
	cr.Spec.Configuration = &api.Configuration{Clusters: []*api.Cluster{{Name: "c"}}}
	normalized, err := chiNormalizer.New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[api.ClickHouseInstallation]())
	require.NoError(t, err)
	return normalized
}

// newBackupSecretWorker wires a worker the way a reconcile does: a fresh task, and thus a fresh creator
// generating fresh random values, on every call.
func newBackupSecretWorker(fake *backupSecretFakeSecret, cr *api.ClickHouseInstallation) *worker {
	w := &worker{
		c: &Controller{kube: &backupSecretFakeKube{secret: fake}},
		a: a.NewAnnouncer(nil, nil),
	}
	w.task = common.NewTask(w.buildCreator(cr), w.buildCreator(nil))
	return w
}

// The backup Secret is created once with random credentials and never regenerated: ClickHouse, the
// sidecars and the backup Jobs pick its values up at different times, so a changed password would lock
// one out of the other. An existing Secret only gets the keys it lacks.
func TestReconcileBackupSecretKeepsValues(t *testing.T) {
	ctx := context.Background()
	cr := newBackupSecretCR(t, enabledBackup())
	fake := &backupSecretFakeSecret{secrets: map[string]*core.Secret{}}
	secretName := cr.GetName() + "-auto-backup-secret"

	w := newBackupSecretWorker(fake, cr)
	require.NoError(t, w.reconcileBackupSecret(ctx, cr))
	require.Equal(t, 1, fake.creates)
	created := fake.secrets[secretName]
	require.NotNil(t, created, "the backup Secret must be created")
	require.True(t, w.task.RegistryReconciled().HasSecret(created.GetObjectMeta()), "an unregistered Secret is purged as unknown")
	require.Len(t, created.OwnerReferences, 1)
	require.Equal(t, cr.GetName(), created.OwnerReferences[0].Name, "the Secret is owned by the installation")

	data := created.Data
	require.Equal(t, config.ClickHouseBackupUsername, string(data[config.ClickHouseBackupSecretKeyAPIUsername]))
	require.Equal(t, config.ClickHouseBackupUsername, string(data[config.ClickHouseBackupSecretKeyClickHouseUsername]))
	apiPassword := string(data[config.ClickHouseBackupSecretKeyAPIPassword])
	chPassword := string(data[config.ClickHouseBackupSecretKeyClickHousePassword])
	require.GreaterOrEqual(t, len(apiPassword), 20)
	require.GreaterOrEqual(t, len(chPassword), 20)
	require.NotEqual(t, apiPassword, chPassword, "the two passwords must be independent")
	initial := maps.Clone(data)

	// The next reconcile generates new candidate values, which must not replace the existing ones
	w = newBackupSecretWorker(fake, cr)
	require.NoError(t, w.reconcileBackupSecret(ctx, cr))
	require.Equal(t, 1, fake.creates)
	require.Zero(t, fake.updates, "a complete Secret needs no update")
	require.Equal(t, initial, fake.secrets[secretName].Data)
	require.True(t, w.task.RegistryReconciled().HasSecret(created.GetObjectMeta()))

	// A Secret missing a key, or holding it empty, gets just that key
	delete(fake.secrets[secretName].Data, config.ClickHouseBackupSecretKeyAPIPassword)
	fake.secrets[secretName].Data[config.ClickHouseBackupSecretKeyClickHouseUsername] = nil
	w = newBackupSecretWorker(fake, cr)
	require.NoError(t, w.reconcileBackupSecret(ctx, cr))
	require.Equal(t, 1, fake.updates)
	repaired := fake.secrets[secretName].Data
	require.NotEmpty(t, repaired[config.ClickHouseBackupSecretKeyAPIPassword])
	require.Equal(t, config.ClickHouseBackupUsername, string(repaired[config.ClickHouseBackupSecretKeyClickHouseUsername]))
	require.Equal(t, initial[config.ClickHouseBackupSecretKeyClickHousePassword], repaired[config.ClickHouseBackupSecretKeyClickHousePassword])
	require.Equal(t, initial[config.ClickHouseBackupSecretKeyAPIUsername], repaired[config.ClickHouseBackupSecretKeyAPIUsername])
}

// Only NotFound proves the Secret is absent. Any other error reading it means "unable to tell", and
// recreating the Secret then would replace live credentials.
func TestReconcileBackupSecretGetError(t *testing.T) {
	cr := newBackupSecretCR(t, enabledBackup())
	fake := &backupSecretFakeSecret{
		secrets: map[string]*core.Secret{},
		getErr:  apiErrors.NewInternalError(errors.New("etcd unavailable")),
	}

	w := newBackupSecretWorker(fake, cr)
	require.Error(t, w.reconcileBackupSecret(context.Background(), cr))
	require.Zero(t, fake.creates)
	require.Zero(t, fake.updates)
	require.Equal(t, 1, w.task.RegistryFailed().NumSecret())
}

// Without any backup-enabled cluster there is no backup Secret to reconcile.
func TestReconcileBackupSecretDisabled(t *testing.T) {
	cr := newBackupSecretCR(t, nil)
	fake := &backupSecretFakeSecret{secrets: map[string]*core.Secret{}}

	w := newBackupSecretWorker(fake, cr)
	require.NoError(t, w.reconcileBackupSecret(context.Background(), cr))
	require.Zero(t, fake.gets)
	require.Empty(t, fake.secrets)
}

// The normalizer takes the backup user's password from the backup Secret, so the Secret is made complete
// before the CR is normalized for reconcile, and a CR normalized before that is normalized anew - once,
// and only when the Secret actually changed, which is reported so that the reconcile goes on to apply it.
func TestPrepareBackupSecretNormalizesAnew(t *testing.T) {
	ctx := context.Background()
	fake := &backupSecretFakeSecret{secrets: map[string]*core.Secret{}}
	normalizer := chiNormalizer.New(func(namespace, name string) (*core.Secret, error) {
		return fake.Get(ctx, &core.Secret{ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: name}})
	})

	raw := &api.ClickHouseInstallation{}
	raw.Name = "chi-backup"
	raw.Namespace = "ns"
	raw.Spec.Configuration = &api.Configuration{Clusters: []*api.Cluster{
		{Name: "c", Backup: enabledBackup()},
	}}
	secretName := raw.GetName() + "-auto-backup-secret"
	normalizations := 0
	normalize := func() *api.ClickHouseInstallation {
		normalizations++
		cr, err := normalizer.CreateTemplated(raw, commonNormalizer.NewOptions[api.ClickHouseInstallation]())
		require.NoError(t, err)
		return cr
	}
	backupUserHash := func(cr *api.ClickHouseInstallation) string {
		return cr.GetSpecT().GetConfiguration().GetUsers().Get(config.ClickHouseBackupUsername + "/password_sha256_hex").String()
	}
	prepare := func(cr *api.ClickHouseInstallation, wantChanged bool) *api.ClickHouseInstallation {
		w := newBackupSecretWorker(fake, cr)
		prepared, changed := w.prepareBackupSecret(ctx, cr, normalize)
		require.Equal(t, wantChanged, changed, "Secret reported changed")
		return prepared
	}

	// First reconcile: the Secret does not exist yet, so the CR comes without the backup user
	cr := normalize()
	require.Empty(t, backupUserHash(cr))
	cr = prepare(cr, true)
	require.Equal(t, 1, fake.creates)
	require.Equal(t, 2, normalizations, "a CR normalized before the Secret existed must be normalized anew")
	password := fake.secrets[secretName].Data[config.ClickHouseBackupSecretKeyClickHousePassword]
	hash := sha256.Sum256(password)
	require.Equal(t, hex.EncodeToString(hash[:]), backupUserHash(cr), "the backup user authenticates with the Secret's password")
	initial := maps.Clone(fake.secrets[secretName].Data)

	// Next reconcile: the Secret is complete, nothing is created and nothing is normalized anew
	cr = normalize()
	require.Same(t, cr, prepare(cr, false))
	require.Equal(t, 3, normalizations)
	require.Equal(t, 1, fake.creates)
	require.Zero(t, fake.updates)
	require.Equal(t, initial, fake.secrets[secretName].Data)

	// A Secret that lost a key gets just that key back, and the user's password stays what it was
	delete(fake.secrets[secretName].Data, config.ClickHouseBackupSecretKeyAPIPassword)
	cr = prepare(normalize(), true)
	require.Equal(t, 1, fake.updates)
	require.Equal(t, 5, normalizations)
	require.Equal(t, hex.EncodeToString(hash[:]), backupUserHash(cr))
	require.Equal(t, initial[config.ClickHouseBackupSecretKeyClickHousePassword], fake.secrets[secretName].Data[config.ClickHouseBackupSecretKeyClickHousePassword])
}

// A suspended CR is not reconciled, so its backup Secret is not created or completed either: the sidecars would restart
// with the new values while the users config keeps the old password, locking them out of ClickHouse until the CR is
// resumed - which reconciles it, the Secret included.
func TestPrepareBackupSecretLeavesSuspendedCR(t *testing.T) {
	cr := newBackupSecretCR(t, enabledBackup())
	cr.Spec.Suspend = types.NewStringBool(true)
	fake := &backupSecretFakeSecret{secrets: map[string]*core.Secret{}}

	w := newBackupSecretWorker(fake, cr)
	prepared, changed := w.prepareBackupSecret(context.Background(), cr, func() *api.ClickHouseInstallation {
		require.Fail(t, "a suspended CR is not normalized anew")
		return nil
	})
	require.Same(t, cr, prepared)
	require.False(t, changed)
	require.Zero(t, fake.gets)
	require.Empty(t, fake.secrets)
}
