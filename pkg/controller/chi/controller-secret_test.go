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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	kubeFake "k8s.io/client-go/kubernetes/fake"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	commonTypes "github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	chopFake "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned/fake"
	chopInformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/cmd_queue"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chiCreator "github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// backupSecretOwnerFakeKube serves CR() alone, from the CRs by name. IKube is embedded as a nil interface, so
// reaching any other accessor panics.
type backupSecretOwnerFakeKube struct {
	interfaces.IKube
	crs map[string]*api.ClickHouseInstallation
}

func (k *backupSecretOwnerFakeKube) CR() interfaces.IKubeCR {
	return &backupSecretOwnerFakeCR{crs: k.crs}
}

type backupSecretOwnerFakeCR struct {
	crs map[string]*api.ClickHouseInstallation
}

func (f *backupSecretOwnerFakeCR) Get(ctx context.Context, namespace, name string) (api.ICustomResource, error) {
	cr, ok := f.crs[name]
	if !ok {
		return nil, apiErrors.NewNotFound(api.Resource(api.ClickHouseInstallationCRDResourceKind), name)
	}
	return cr.DeepCopy(), nil
}

func (f *backupSecretOwnerFakeCR) StatusUpdate(ctx context.Context, cr api.ICustomResource, opts commonTypes.UpdateStatusOptions) error {
	panic("a deleted Secret must not update the status of a CR")
}

// newBackupSecretOwner is a CR whose normalized form, completed or being reconciled, has backup enabled or not
func newBackupSecretOwner(name string, completedWithBackup, reconciledWithBackup *bool) *api.ClickHouseInstallation {
	normalized := func(backup bool) *api.ClickHouseInstallation {
		cr := &api.ClickHouseInstallation{}
		cr.Spec.Backup = &api.ClusterBackup{
			Enabled:  commonTypes.NewStringBool(backup),
			Storages: []api.BackupStorage{{Name: "main", SecretRef: &core.LocalObjectReference{Name: testStorageSecret}}},
		}
		cr.Spec.Configuration = &api.Configuration{Clusters: []*api.Cluster{{Name: "c"}}}
		return cr
	}
	cr := &api.ClickHouseInstallation{}
	cr.Name = name
	cr.Namespace = "ns"
	cr.UID = kubeTypes.UID("uid-" + name)
	if completedWithBackup != nil {
		cr.EnsureStatus().NormalizedCRCompleted = normalized(*completedWithBackup)
	}
	if reconciledWithBackup != nil {
		cr.EnsureStatus().NormalizedCR = normalized(*reconciledWithBackup)
	}
	return cr
}

// newOwnedSecret is a Secret of the name that the CR controls, labeled as the operator labels its objects
func newOwnedSecret(cr *api.ClickHouseInstallation, name string) *core.Secret {
	secret := &core.Secret{ObjectMeta: meta.ObjectMeta{
		Namespace:       cr.GetNamespace(),
		Name:            name,
		Labels:          chiLabeler.New(cr).Label(interfaces.LabelSecretBackup),
		OwnerReferences: chiCreator.NewOwnerReferencer().CreateOwnerReferences(cr),
	}}
	return secret
}

// queuedCRs are the names of the CRs whose reconcile the controller's queues hold
func queuedCRs(t *testing.T, c *Controller) []string {
	t.Helper()
	var names []string
	for _, q := range c.queues {
		for q.Len() > 0 {
			item, _, ok := q.Get()
			require.True(t, ok)
			command, ok := item.(*cmd_queue.ReconcileCHI)
			require.True(t, ok, "unexpected queue item %T", item)
			require.Equal(t, cmd_queue.ReconcileAdd, command.Cmd, "the reconcile must not depend on a spec diff")
			names = append(names, command.New.GetName())
			q.Done(item)
		}
	}
	return names
}

// Deleting the backup Secret of a CR that uses backup reconciles the CR, which creates the Secret anew: otherwise the
// Secret stays missing until something else reconciles the CR, and no backup or restore Job starts. The deletion of
// any other Secret reconciles nothing, and neither does the deletion of the backup Secret by a reconcile that turns
// backup off or by the deletion of the CR, as the reconcile running then would be cancelled.
func TestBackupSecretDeletionReconcilesOwner(t *testing.T) {
	yes, no := true, false
	backupSecretName := func(cr *api.ClickHouseInstallation) string {
		return managers.NewNameManager(managers.NameManagerTypeClickHouse).Name(interfaces.NameBackupAutoSecret, cr)
	}
	beingDeleted := newBackupSecretOwner("being-deleted", &yes, nil)
	beingDeleted.DeletionTimestamp = &meta.Time{Time: time.Now()}
	recreated := newBackupSecretOwner("recreated", &yes, nil)
	gone := newBackupSecretOwner("gone", &yes, nil)

	for name, tc := range map[string]struct {
		owner *api.ClickHouseInstallation
		// stored tells whether the API server still has the owner as it is
		stored bool
		secret func(owner *api.ClickHouseInstallation) *core.Secret
		want   bool
	}{
		"backup Secret of a CR using backup": {
			owner:  newBackupSecretOwner("completed", &yes, nil),
			stored: true,
			want:   true,
		},
		"backup Secret of a CR being reconciled with backup": {
			owner:  newBackupSecretOwner("reconciled", &no, &yes),
			stored: true,
			want:   true,
		},
		"backup Secret of a CR being reconciled without backup": {
			owner:  newBackupSecretOwner("turned-off", &yes, &no),
			stored: true,
		},
		"backup Secret of a CR without backup": {
			owner:  newBackupSecretOwner("never", nil, nil),
			stored: true,
		},
		"backup Secret of a CR being deleted": {
			owner:  beingDeleted,
			stored: true,
		},
		"backup Secret of a CR deleted since": {
			owner: gone,
		},
		"backup Secret of a CR since created anew": {
			owner: recreated,
			secret: func(owner *api.ClickHouseInstallation) *core.Secret {
				old := owner.DeepCopy()
				old.UID = "uid-before"
				return newOwnedSecret(old, backupSecretName(old))
			},
			stored: true,
		},
		"other Secret of a CR using backup": {
			owner:  newBackupSecretOwner("other", &yes, nil),
			stored: true,
			secret: func(owner *api.ClickHouseInstallation) *core.Secret {
				return newOwnedSecret(owner, owner.GetName()+"-cluster-auto-secret")
			},
		},
		"Secret named as a backup Secret, not controlled by a CR": {
			owner:  newBackupSecretOwner("uncontrolled", &yes, nil),
			stored: true,
			secret: func(owner *api.ClickHouseInstallation) *core.Secret {
				secret := newOwnedSecret(owner, backupSecretName(owner))
				secret.OwnerReferences = nil
				return secret
			},
		},
		"backup Secret without the operator's label": {
			owner:  newBackupSecretOwner("unlabeled", &yes, nil),
			stored: true,
			secret: func(owner *api.ClickHouseInstallation) *core.Secret {
				secret := newOwnedSecret(owner, backupSecretName(owner))
				secret.Labels = nil
				return secret
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			// Deleted after the case's Secret, it tells that the informer has handled that deletion
			sentinel := newBackupSecretOwner("sentinel", &yes, nil)
			crs := map[string]*api.ClickHouseInstallation{sentinel.GetName(): sentinel}
			if tc.stored {
				crs[tc.owner.GetName()] = tc.owner
			}
			secret := newOwnedSecret(tc.owner, backupSecretName(tc.owner))
			if tc.secret != nil {
				secret = tc.secret(tc.owner)
			}
			sentinelSecret := newOwnedSecret(sentinel, backupSecretName(sentinel))

			c := &Controller{
				kube:  &backupSecretOwnerFakeKube{crs: crs},
				namer: managers.NewNameManager(managers.NameManagerTypeClickHouse),
			}
			c.initQueues()
			client := kubeFake.NewSimpleClientset(secret, sentinelSecret)
			factory := NewInformerFactoryForCHOPGeneratedObjects(client, 0)
			chopFactory := func() chopInformers.SharedInformerFactory {
				return chopInformers.NewSharedInformerFactory(chopFake.NewSimpleClientset(), 0)
			}
			c.addEventHandlers(chopFactory(), chopFactory(), factory)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer factory.Shutdown()
			defer cancel()
			factory.Start(ctx.Done())
			for informer, synced := range factory.WaitForCacheSync(ctx.Done()) {
				require.True(t, synced, "informer of %v must sync", informer)
			}

			secrets := client.CoreV1().Secrets(secret.GetNamespace())
			require.NoError(t, secrets.Delete(ctx, secret.GetName(), meta.DeleteOptions{}))
			require.NoError(t, secrets.Delete(ctx, sentinelSecret.GetName(), meta.DeleteOptions{}))
			// The handler takes the deletions in order, so the sentinel's reconcile is enqueued after the case's one
			want := []string{sentinel.GetName()}
			if tc.want {
				want = []string{tc.owner.GetName(), sentinel.GetName()}
			}
			require.Eventually(t, func() bool {
				queued := 0
				for _, q := range c.queues {
					queued += q.Len()
				}
				return queued >= len(want)
			}, 5*time.Second, 10*time.Millisecond, "the deletions did not reconcile %v", want)
			require.ElementsMatch(t, want, queuedCRs(t, c))
		})
	}
}
