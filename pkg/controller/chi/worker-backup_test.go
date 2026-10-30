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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/backupconfig"
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
	events := &recordingEventEmitter{}
	w := newBackupSecretWorker(&backupSecretFakeSecret{secrets: secrets}, cr)
	w.a = a.NewAnnouncer(events, nil)
	require.NoError(t, w.reconcileCRAuxObjectsPreliminaryDomain(context.Background(), cr))
	return events
}

// A storage Secret that is missing, or holds no configuration file, leaves the configuration file of the sidecars
// without a storage - and every backup Job of the cluster fails. A Warning event on the installation is what tells
// the user why. It repeats on every reconcile, so it has a reason of its own: one alerting can tell from a failed
// reconcile. Storage settings that cannot be read leave the configuration file as it is, with a reason of their own.
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
		reason  string
		message string
	}{
		"missing": {
			secrets: map[string]*core.Secret{},
			reason:  a.EventReasonBackupStorageSecretMissing,
			message: "does not exist",
		},
		"without a configuration file": {
			secrets: storage(map[string]string{"REMOTE_STORAGE": "s3"}),
			reason:  a.EventReasonBackupStorageSecretMissing,
			message: "has no key " + backupconfig.FileName,
		},
		"with a configuration file that cannot be read": {
			secrets: storage(map[string]string{backupconfig.FileName: "s3: ["}),
			reason:  a.EventReasonBackupStorageConfigInvalid,
			message: "cannot be read",
		},
	} {
		t.Run(name, func(t *testing.T) {
			warnings := reconcileBackupAuxObjects(t, newCR(nil), tc.secrets).warnings()
			require.Len(t, warnings, 1)
			require.Equal(t, tc.reason, warnings[0].reason)
			require.Contains(t, warnings[0].message, testStorageSecret)
			require.Contains(t, warnings[0].message, tc.message)
		})
	}
	t.Run("present", func(t *testing.T) {
		require.Empty(t, reconcileBackupAuxObjects(t, newCR(nil), testStorageSecrets()).warnings())
	})
	t.Run("cluster opts out of backup", func(t *testing.T) {
		require.Empty(t, reconcileBackupAuxObjects(t, newCR(&api.ClusterBackup{Enabled: types.NewStringBool(false)}), map[string]*core.Secret{}).warnings())
	})
}

// Backup turned on without a storage stays off: the hosts run without the sidecar, and backups of the
// cluster are refused. One Warning event on the installation names every such cluster and how to turn backup on.
// It repeats on every reconcile, so it has a reason of its own: one alerting can tell from a failed reconcile.
func TestBackupWithoutStorageWarning(t *testing.T) {
	newCR := func(backups ...*api.ClusterBackup) *api.ClickHouseInstallation {
		cr := &api.ClickHouseInstallation{}
		cr.Name = "chi-backup"
		cr.Namespace = "ns"
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
	secrets := testStorageSecrets()

	for name, tc := range map[string]struct {
		backups []*api.ClusterBackup
		names   string
	}{
		"one cluster":             {backups: []*api.ClusterBackup{lacking()}, names: "without a storage: c0 - "},
		"two clusters, one event": {backups: []*api.ClusterBackup{lacking(), enabledBackup(), lacking()}, names: "without a storage: c0, c2 - "},
		"a storage of no Secret on a cluster": {
			backups: []*api.ClusterBackup{{Enabled: types.NewStringBool(true), Storages: []api.BackupStorage{{Name: "main"}}}},
			names:   "without a storage: c0 - ",
		},
	} {
		t.Run(name, func(t *testing.T) {
			warnings := reconcileBackupAuxObjects(t, newCR(tc.backups...), secrets).warnings()
			require.Len(t, warnings, 1)
			require.Equal(t, a.EventReasonBackupNoStorage, warnings[0].reason)
			require.Contains(t, warnings[0].message, tc.names)
			require.Contains(t, warnings[0].message, api.BackupStorageHint)
		})
	}
	t.Run("backup with a storage, or off", func(t *testing.T) {
		cr := newCR(enabledBackup(), &api.ClusterBackup{Enabled: types.NewStringBool(false)}, nil)
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
