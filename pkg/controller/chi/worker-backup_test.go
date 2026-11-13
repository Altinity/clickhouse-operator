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
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/backupconfig"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	chiNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

// recordedEvent is a k8s event as the announcer emits it
type recordedEvent struct {
	level   string
	reason  string
	message string
}

// recordingEventEmitter keeps the events it is given instead of sending them to the API server
type recordingEventEmitter struct {
	events []recordedEvent
}

func (r *recordingEventEmitter) Event(level string, obj meta.Object, action string, reason string, message string) {
	r.events = append(r.events, recordedEvent{level: level, reason: reason, message: message})
}

func (r *recordingEventEmitter) EventInfo(obj meta.Object, action string, reason string, message string) {
	r.Event(eventLevelInfo, obj, action, reason, message)
}

func (r *recordingEventEmitter) EventWarning(obj meta.Object, action string, reason string, message string) {
	r.Event(eventLevelWarning, obj, action, reason, message)
}

func (r *recordingEventEmitter) EventError(obj meta.Object, action string, reason string, message string) {
	r.Event(eventLevelError, obj, action, reason, message)
}

const (
	eventLevelInfo    = "info"
	eventLevelWarning = "warning"
	eventLevelError   = "error"
)

// warnings lists the warning events
func (r *recordingEventEmitter) warnings() (warnings []recordedEvent) {
	for _, event := range r.events {
		if event.level == eventLevelWarning {
			warnings = append(warnings, event)
		}
	}
	return warnings
}

// testStorageSecrets holds the storage Secret enabledBackup names, with storage settings
func testStorageSecrets() map[string]*core.Secret {
	return map[string]*core.Secret{testStorageSecret: {
		ObjectMeta: meta.ObjectMeta{Name: testStorageSecret},
		Data:       map[string][]byte{backupconfig.FileName: []byte("general:\n  remote_storage: s3\n")},
	}}
}

// reconcileBackupAuxObjects runs the backup part of the preliminary reconcile of the CR, recording its events
func reconcileBackupAuxObjects(t *testing.T, cr *api.ClickHouseInstallation, secrets map[string]*core.Secret) *recordingEventEmitter {
	t.Helper()
	events, err := reconcileBackupAuxObjectsErr(t, cr, secrets)
	require.NoError(t, err)
	return events
}

// reconcileBackupAuxObjectsErr is reconcileBackupAuxObjects that returns the error of the reconcile
func reconcileBackupAuxObjectsErr(t *testing.T, cr *api.ClickHouseInstallation, secrets map[string]*core.Secret) (*recordingEventEmitter, error) {
	t.Helper()
	events := &recordingEventEmitter{}
	w := newBackupSecretWorker(&backupSecretFakeSecret{secrets: secrets}, cr)
	w.a = a.NewAnnouncer(events, nil)
	return events, w.reconcileCRAuxObjectsPreliminaryDomain(context.Background(), cr)
}

// requireBackupConfigAbort checks that the reconcile was aborted as the backup cannot be configured: the error stops
// the reconcile, the status of the installation is Aborted with the reason, and one Warning event tells why
func requireBackupConfigAbort(t *testing.T, cr *api.ClickHouseInstallation, events *recordingEventEmitter, err error, message string) {
	t.Helper()
	require.ErrorIs(t, err, common.ErrCRUDAbort)
	require.Equal(t, api.StatusAborted, cr.EnsureStatus().GetStatus())
	require.NotEmpty(t, cr.EnsureStatus().GetErrors())
	require.True(t, strings.HasPrefix(cr.EnsureStatus().GetErrors()[0], "["+api.StatusReasonBackupConfigInvalid+"]"))
	require.Contains(t, cr.EnsureStatus().GetErrors()[0], message)
	warnings := events.warnings()
	require.Len(t, warnings, 1)
	require.Equal(t, a.EventReasonBackupStorageConfigInvalid, warnings[0].reason)
	require.Contains(t, warnings[0].message, message)
}

// A storage Secret that is missing, holds no configuration file, an empty one, or one that cannot be read aborts the
// reconcile of the installation: a cluster backed up without a usable storage would fail every backup and restore.
// The status of the installation tells the reason, BackupConfigInvalid, and a Warning event on it tells why.
func TestBackupStorageSecretMissing(t *testing.T) {
	newCR := func(backup *api.ClusterBackup) *api.ClickHouseInstallation {
		cr := newBackupSecretCR(t, enabledBackup())
		cr.FindCluster("c").(*api.Cluster).Backup = backup.MergeFrom(cr.Spec.Backup, api.MergeTypeFillEmptyValues)
		return cr
	}
	storage := func(data map[string]string) map[string]*core.Secret {
		secret := &core.Secret{ObjectMeta: meta.ObjectMeta{Name: testStorageSecret}, Data: map[string][]byte{}}
		for key, value := range data {
			secret.Data[key] = []byte(value)
		}
		return map[string]*core.Secret{testStorageSecret: secret}
	}

	for name, tc := range map[string]struct {
		secrets map[string]*core.Secret
		message string
	}{
		"missing": {
			secrets: map[string]*core.Secret{},
			message: "does not exist",
		},
		"without a configuration file": {
			secrets: storage(map[string]string{"REMOTE_STORAGE": "s3"}),
			message: "has no key " + backupconfig.FileName,
		},
		"with an empty configuration file": {
			secrets: storage(map[string]string{backupconfig.FileName: " \n"}),
			message: "has an empty key " + backupconfig.FileName,
		},
		"with a configuration file that cannot be read": {
			secrets: storage(map[string]string{backupconfig.FileName: "s3: ["}),
			message: "cannot be read",
		},
		"with a configuration file whose section is no mapping": {
			secrets: storage(map[string]string{backupconfig.FileName: "s3: bucket\n"}),
			message: "cannot be read",
		},
	} {
		t.Run(name, func(t *testing.T) {
			cr := newCR(nil)
			events, err := reconcileBackupAuxObjectsErr(t, cr, tc.secrets)
			requireBackupConfigAbort(t, cr, events, err, tc.message)
			require.Contains(t, events.warnings()[0].message, testStorageSecret)
		})
	}
	t.Run("present", func(t *testing.T) {
		require.Empty(t, reconcileBackupAuxObjects(t, newCR(nil), testStorageSecrets()).warnings())
	})
	t.Run("cluster opts out of backup", func(t *testing.T) {
		require.Empty(t, reconcileBackupAuxObjects(t, newCR(&api.ClusterBackup{Enabled: types.NewStringBool(false)}), map[string]*core.Secret{}).warnings())
	})
}

// Backup turned on without a storage aborts the reconcile: backup takes explicit configuration. The status and one
// Warning event on the installation name every such cluster and how to turn backup on. A cluster that turns backup
// off, or names a storage, is never a problem.
func TestBackupWithoutStorageAborts(t *testing.T) {
	newCR := func(installation *api.ClusterBackup, backups ...*api.ClusterBackup) *api.ClickHouseInstallation {
		cr := &api.ClickHouseInstallation{}
		cr.Name = "chi-backup"
		cr.Namespace = "ns"
		cr.Spec.Backup = installation
		cr.Spec.Configuration = &api.Configuration{}
		for i, backup := range backups {
			cr.Spec.Configuration.Clusters = append(cr.Spec.Configuration.Clusters, &api.Cluster{Name: "c" + strconv.Itoa(i), Backup: backup})
		}
		normalized, err := chiNormalizer.New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[api.ClickHouseInstallation]())
		require.NoError(t, err)
		return normalized
	}
	lacking := func() *api.ClusterBackup {
		return &api.ClusterBackup{Enabled: types.NewStringBool(true)}
	}
	off := func() *api.ClusterBackup {
		return &api.ClusterBackup{Enabled: types.NewStringBool(false)}
	}
	secrets := testStorageSecrets()

	for name, tc := range map[string]struct {
		cr    *api.ClickHouseInstallation
		names string
	}{
		"one cluster":             {cr: newCR(nil, lacking()), names: "without a storage on the clusters: c0 - "},
		"two clusters, one event": {cr: newCR(nil, lacking(), enabledBackup(), lacking()), names: "without a storage on the clusters: c0, c2 - "},
		"a storage of no Secret on a cluster": {
			cr:    newCR(nil, &api.ClusterBackup{Enabled: types.NewStringBool(true), ConfigSecretRef: &core.LocalObjectReference{}}),
			names: "without a storage on the clusters: c0 - ",
		},
		"inherited from the installation": {cr: newCR(lacking(), nil, off()), names: "without a storage on the clusters: c0 - "},
	} {
		t.Run(name, func(t *testing.T) {
			events, err := reconcileBackupAuxObjectsErr(t, tc.cr, secrets)
			requireBackupConfigAbort(t, tc.cr, events, err, tc.names)
			require.Contains(t, events.warnings()[0].message, api.BackupStorageHint)
		})
	}
	t.Run("backup with a storage, or off", func(t *testing.T) {
		cr := newCR(nil, enabledBackup(), off(), nil)
		require.Empty(t, reconcileBackupAuxObjects(t, cr, secrets).warnings())
	})
	t.Run("every cluster opts out of the installation's backup", func(t *testing.T) {
		cr := newCR(lacking(), off(), off())
		require.Empty(t, reconcileBackupAuxObjects(t, cr, secrets).warnings())
	})
}

// A host Service reachable from outside the cluster does not publish the sidecar's plain-HTTP API, so the backup
// Jobs cannot reach that host. A Warning event on the installation names the hosts. It repeats on every
// reconcile, so it has a reason of its own: one alerting can tell from a failed reconcile.
func TestBackupAPIUnpublishedWarning(t *testing.T) {
	const hostServiceTemplate = "host-svc"
	newCR := func(spec core.ServiceSpec) *api.ClickHouseInstallation {
		cr := &api.ClickHouseInstallation{}
		cr.Name = "chi-backup"
		cr.Namespace = "ns"
		cr.Spec.Backup = enabledBackup()
		cr.Spec.Templates = &api.Templates{ServiceTemplates: []api.ServiceTemplate{{
			Name: hostServiceTemplate,
			Spec: spec,
		}}}
		cr.Spec.Configuration = &api.Configuration{Clusters: []*api.Cluster{{
			Name:      "c",
			Templates: &api.TemplatesList{ReplicaServiceTemplate: hostServiceTemplate},
		}}}
		normalized, err := chiNormalizer.New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[api.ClickHouseInstallation]())
		require.NoError(t, err)
		return normalized
	}
	warnings := func(t *testing.T, cr *api.ClickHouseInstallation) []recordedEvent {
		t.Helper()
		secrets := testStorageSecrets()
		return reconcileBackupAuxObjects(t, cr, secrets).warnings()
	}

	for name, spec := range map[string]core.ServiceSpec{
		"LoadBalancer":            {Type: core.ServiceTypeLoadBalancer},
		"ClusterIP, external IPs": {Type: core.ServiceTypeClusterIP, ExternalIPs: []string{"203.0.113.10"}},
	} {
		t.Run(name, func(t *testing.T) {
			cr := newCR(spec)
			got := warnings(t, cr)
			require.Len(t, got, 1)
			require.Equal(t, a.EventReasonBackupAPIUnpublished, got[0].reason)
			cr.WalkHosts(func(host *api.Host) error {
				require.Contains(t, got[0].message, host.GetName())
				return nil
			})
		})
	}
	t.Run("ClusterIP", func(t *testing.T) {
		require.Empty(t, warnings(t, newCR(core.ServiceSpec{Type: core.ServiceTypeClusterIP})))
	})
}

// Storage settings that cannot be read leave the configuration file Secret as it is, on purpose: it stays registered
// as reconciled, so that the cleanup does not purge it as an unknown object, or as one that failed to reconcile
func TestBackupConfigSecretKeptIsRegistered(t *testing.T) {
	ctx := context.Background()
	cr := newBackupSecretCR(t, enabledBackup())
	fake := &backupSecretFakeSecret{secrets: map[string]*core.Secret{testStorageSecret: storageSecretWith("s3:\n  bucket: first\n")}}
	require.NoError(t, newBackupSecretWorker(fake, cr).reconcileCRAuxObjectsPreliminaryDomain(ctx, cr))
	require.Contains(t, fake.secrets, configSecretName)

	fake.secrets[testStorageSecret] = storageSecretWith("s3: [")
	w := newBackupSecretWorker(fake, cr)
	require.ErrorIs(t, w.reconcileCRAuxObjectsPreliminaryDomain(ctx, cr), common.ErrCRUDAbort)
	kept := &meta.ObjectMeta{Namespace: cr.GetNamespace(), Name: configSecretName}
	require.True(t, w.task.RegistryReconciled().HasSecret(kept))
	require.False(t, w.task.RegistryFailed().HasSecret(kept))
	require.Equal(t, "first", configFile(t, fake.secrets[configSecretName])["s3"]["bucket"])
}

// A backup Secret that sets only one of the ClickHouse username and password gets no backup user, which a Warning
// event on the installation tells, naming what is set and what is not. Both set, or neither, is no mistake.
func TestBackupCredentialsIncomplete(t *testing.T) {
	for name, tc := range map[string]struct {
		data    map[string]string
		message []string
	}{
		"neither": {},
		"both": {data: map[string]string{
			config.ClickHouseBackupSecretKeyClickHouseUsername: testBackupUsername,
			config.ClickHouseBackupSecretKeyClickHousePassword: "password",
		}},
		"username only": {
			data:    map[string]string{config.ClickHouseBackupSecretKeyClickHouseUsername: testBackupUsername},
			message: []string{"sets CLICKHOUSE_USERNAME without CLICKHOUSE_PASSWORD"},
		},
		"password only": {
			data:    map[string]string{config.ClickHouseBackupSecretKeyClickHousePassword: "password"},
			message: []string{"sets CLICKHOUSE_PASSWORD without CLICKHOUSE_USERNAME"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			secrets := map[string]*core.Secret{
				testStorageSecret: storageSecretWith("s3:\n  bucket: a\n"),
				"chi-backup-auto-backup-secret": {
					ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "chi-backup-auto-backup-secret"},
					Data:       map[string][]byte{},
				},
			}
			for key, value := range tc.data {
				secrets["chi-backup-auto-backup-secret"].Data[key] = []byte(value)
			}
			var incomplete []string
			for _, warning := range reconcileBackupAuxObjects(t, newBackupSecretCR(t, enabledBackup()), secrets).warnings() {
				if warning.reason == a.EventReasonBackupCredentialsIncomplete {
					incomplete = append(incomplete, warning.message)
				}
			}
			require.Len(t, incomplete, len(tc.message))
			for i, message := range tc.message {
				require.Contains(t, incomplete[i], message)
				require.Contains(t, incomplete[i], "creates no backup user")
			}
		})
	}
}

// A backup block out of range aborts the reconcile before any Secret is made: a mode the operator does not know
// would leave the cluster backed up by neither sidecars nor backup Pods
func TestBackupSpecInvalidAborts(t *testing.T) {
	cr := newBackupSecretCR(t, enabledBackup())
	cr.FindCluster("c").(*api.Cluster).Backup.Mode = types.NewString("daemon")
	secrets := testStorageSecrets()
	events, err := reconcileBackupAuxObjectsErr(t, cr, secrets)
	requireBackupConfigAbort(t, cr, events, err, `backup.mode "daemon"`)
	require.NotContains(t, secrets, configSecretName, "no configuration file is made for a backup that cannot be configured")
}

// An installation whose reconcile was aborted as its backup could not be configured is reconciled anew by the sync,
// once its storage Secrets can be used: the operator watches no storage Secret, so nothing else would reconcile it
func TestBackupStorageUsableAfterAbort(t *testing.T) {
	aborted := func() *api.ClickHouseInstallation {
		cr := newBackupSecretCR(t, enabledBackup())
		cr.EnsureStatus().ReconcileAbortWithReason(api.StatusReasonBackupConfigInvalid, "cluster c: Secret ns/storage does not exist")
		return cr
	}
	require.True(t, isAbortedOnInvalidBackupConfig(aborted()))
	other := newBackupSecretCR(t, enabledBackup())
	other.EnsureStatus().ReconcileAbortWithReason(api.StatusReasonInvalidPodTemplate, "x")
	require.False(t, isAbortedOnInvalidBackupConfig(other), "an abort for another reason is not retried here")
	completed := aborted()
	completed.EnsureStatus().ReconcileComplete()
	require.False(t, isAbortedOnInvalidBackupConfig(completed), "only the last reconcile counts")

	for name, tc := range map[string]struct {
		secrets map[string]*core.Secret
		usable  bool
	}{
		"missing":       {secrets: map[string]*core.Secret{}, usable: false},
		"unreadable":    {secrets: map[string]*core.Secret{testStorageSecret: storageSecretWith("s3: [")}, usable: false},
		"empty":         {secrets: map[string]*core.Secret{testStorageSecret: storageSecretWith(" ")}, usable: false},
		"fixed":         {secrets: testStorageSecrets(), usable: true},
		"section wrong": {secrets: map[string]*core.Secret{testStorageSecret: storageSecretWith("api: [x]\n")}, usable: false},
	} {
		t.Run(name, func(t *testing.T) {
			cr := aborted()
			w := newBackupSecretWorker(&backupSecretFakeSecret{secrets: tc.secrets}, cr)
			require.Equal(t, tc.usable, w.c.backupStorageUsable(context.Background(), cr))
		})
	}
	t.Run("backup block out of range", func(t *testing.T) {
		cr := aborted()
		cr.Spec.Backup.Mode = types.NewString("daemon")
		w := newBackupSecretWorker(&backupSecretFakeSecret{secrets: testStorageSecrets()}, cr)
		require.False(t, w.c.backupStorageUsable(context.Background(), cr))
	})
}

// A cluster in backup.mode pod is backed up by Pods that connect to ClickHouse over the network, which only the
// backup user the operator manages may: without its credentials in the backup Secret the reconcile aborts, naming the
// cluster and the keys to set. A cluster backed up by sidecars connects over localhost and needs none.
func TestBackupPodModeNeedsBackupUserCredentials(t *testing.T) {
	podMode := func() *api.ClickHouseInstallation {
		backup := enabledBackup()
		backup.Mode = types.NewString(api.BackupModePod)
		return newBackupSecretCR(t, backup)
	}
	withCredentials := func(username, password string) map[string]*core.Secret {
		secrets := testStorageSecrets()
		data := map[string][]byte{
			config.ClickHouseBackupSecretKeyAPIUsername: []byte("api"),
			config.ClickHouseBackupSecretKeyAPIPassword: []byte("api-password"),
		}
		if username != "" {
			data[config.ClickHouseBackupSecretKeyClickHouseUsername] = []byte(username)
		}
		if password != "" {
			data[config.ClickHouseBackupSecretKeyClickHousePassword] = []byte(password)
		}
		secrets["chi-backup-auto-backup-secret"] = &core.Secret{
			ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "chi-backup-auto-backup-secret"},
			Data:       data,
		}
		return secrets
	}

	for name, secrets := range map[string]map[string]*core.Secret{
		"none":          testStorageSecrets(),
		"username only": withCredentials("backup_user", ""),
		"password only": withCredentials("", "secret"),
	} {
		t.Run(name, func(t *testing.T) {
			cr := podMode()
			events, err := reconcileBackupAuxObjectsErr(t, cr, secrets)
			// Credentials set halfway are warned about besides, see TestBackupCredentialsIncomplete
			events.events = slices.DeleteFunc(events.events, func(event recordedEvent) bool {
				return event.reason == a.EventReasonBackupCredentialsIncomplete
			})
			requireBackupConfigAbort(t, cr, events, err, "backup.mode pod (c)")
			message := events.warnings()[0].message
			require.Contains(t, message, config.ClickHouseBackupSecretKeyClickHouseUsername)
			require.Contains(t, message, config.ClickHouseBackupSecretKeyClickHousePassword)
			require.Contains(t, message, "ns/chi-backup-auto-backup-secret")
		})
	}
	t.Run("credentials set", func(t *testing.T) {
		cr := podMode()
		events, err := reconcileBackupAuxObjectsErr(t, cr, withCredentials("backup_user", "secret"))
		require.NoError(t, err)
		require.NotEqual(t, api.StatusAborted, cr.EnsureStatus().GetStatus())
		for _, warning := range events.warnings() {
			require.NotEqual(t, a.EventReasonBackupStorageConfigInvalid, warning.reason)
		}
	})
	t.Run("the sync retries once the credentials are set", func(t *testing.T) {
		cr := podMode()
		w := newBackupSecretWorker(&backupSecretFakeSecret{secrets: withCredentials("backup_user", "")}, cr)
		require.False(t, w.c.backupStorageUsable(context.Background(), cr))
		w = newBackupSecretWorker(&backupSecretFakeSecret{secrets: withCredentials("backup_user", "secret")}, cr)
		require.True(t, w.c.backupStorageUsable(context.Background(), cr))
	})
	t.Run("sidecar mode needs none", func(t *testing.T) {
		require.Empty(t, reconcileBackupAuxObjects(t, newBackupSecretCR(t, enabledBackup()), testStorageSecrets()).warnings())
	})
}
