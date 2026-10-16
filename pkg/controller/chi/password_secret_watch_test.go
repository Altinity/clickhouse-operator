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
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
)

func TestChiReferencesHotReloadSecret(t *testing.T) {
	cr := &api.ClickHouseInstallation{}
	cr.Spec.Configuration = api.NewConfiguration()
	cr.Spec.Configuration.Users = api.NewSettings()
	cr.Spec.Configuration.Users.Set("alice/password", api.NewSettingSource(&api.SettingSource{
		ValueFrom: &types.DataSource{
			SecretKeyRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "clickhouse-passwords"},
				Key:                  "alice_password",
			},
			HotReload: true,
		},
	}))
	cr.Spec.Configuration.Users.Set("bob/password", api.NewSettingSource(&api.SettingSource{
		ValueFrom: &types.DataSource{
			SecretKeyRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "other"},
				Key:                  "bob_password",
			},
		},
	}))

	require.True(t, chiReferencesHotReloadSecret(cr, "clickhouse-passwords"))
	require.False(t, chiReferencesHotReloadSecret(cr, "other"))
	require.False(t, chiReferencesHotReloadSecret(cr, "missing"))
}

// A source Secret update does not change CHI generation. The watcher still
// enqueues a reconcile, the gate lets it run, the rendered users document
// changes, and the host is not restarted.
func TestPasswordSecretChangeReconcilesWithoutRestart(t *testing.T) {
	cr := &api.ClickHouseInstallation{}
	cr.Namespace = "own-ns"
	cr.Spec.Configuration = api.NewConfiguration()
	cr.Spec.Configuration.Users = api.NewSettings()
	cr.Spec.Configuration.Users.Set("alice/password", api.NewSettingSource(&api.SettingSource{
		ValueFrom: &types.DataSource{
			SecretKeyRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "clickhouse-passwords"},
				Key:                  "alice_password",
			},
			HotReload: true,
		},
	}))
	require.True(t, chiReferencesHotReloadSecret(cr, "clickhouse-passwords"))

	decision := decideReconcileGate(reconcileGateInputs{
		generationTheSame:           true,
		operatorIPTheSame:           true,
		passwordSecretChanged:       true,
		hasHostNeedingStuckRecovery: alwaysFalse,
	})
	require.Equal(t, gatePasswordSecretChanged, decision)
	require.True(t, decision.proceeds())

	render := func(password string) string {
		xml, err := normalizer.RenderHotReloadUsersXML(cr.Spec.Configuration.Users, cr.Namespace, func(namespace, name string) (*core.Secret, error) {
			return &core.Secret{Data: map[string][]byte{"alice_password": []byte(password)}}, nil
		})
		require.NoError(t, err)
		return xml
	}
	require.NotEqual(t, render("secret-value-1"), render("secret-value-2"))

	w := &worker{}
	host := rollingUpdateHostWithAncestor()
	secretCtx := context.WithValue(context.Background(), passwordSecretReconcileKey{}, true)
	require.False(t, w.shouldForceRestartHost(secretCtx, host))
	require.True(t, w.shouldForceRestartHost(context.Background(), host))

	current := host.GetCR().(*api.ClickHouseInstallation)
	current.EnsureRuntime().ActionPlan = api.MakeActionPlan(current.GetAncestorT(), current)
	require.True(t, w.shouldForceRestartHost(secretCtx, host))
}

func TestPasswordSecretRefreshRestartsUnhealthyHost(t *testing.T) {
	secretCtx := context.WithValue(context.Background(), passwordSecretReconcileKey{}, true)

	t.Run("crash loop", func(t *testing.T) {
		host := rollingUpdateHostWithAncestor()
		host.Runtime.Version = nil
		w := workerWithPod(&core.Pod{
			Status: core.PodStatus{
				Phase: core.PodRunning,
				ContainerStatuses: []core.ContainerStatus{{
					State: core.ContainerState{
						Waiting: &core.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
					},
				}},
			},
		})
		require.True(t, w.shouldForceRestartHost(secretCtx, host))
	})

	t.Run("sustained not ready", func(t *testing.T) {
		cfg := chop.Config()
		prev := cfg.Reconcile.Recovery.OnStatus.Completed.OnPodNotReady
		cfg.Reconcile.Recovery.OnStatus.Completed.OnPodNotReady = types.NewString(api.RecoveryActionRetry)
		t.Cleanup(func() {
			cfg.Reconcile.Recovery.OnStatus.Completed.OnPodNotReady = prev
		})

		host := rollingUpdateHostWithAncestor()
		w := workerWithPod(&core.Pod{
			Status: core.PodStatus{
				Phase: core.PodRunning,
				Conditions: []core.PodCondition{{
					Type:               core.PodReady,
					Status:             core.ConditionFalse,
					LastTransitionTime: meta.NewTime(time.Now().Add(-cfg.CompletedOnPodNotReadyThreshold() - time.Minute)),
				}},
				ContainerStatuses: []core.ContainerStatus{{Ready: false}},
			},
		})
		require.True(t, w.shouldForceRestartHost(secretCtx, host))
	})
}

func workerWithPod(pod *core.Pod) *worker {
	return &worker{c: &Controller{kube: &statusFakeKube{pod: &statusFakePod{pod: pod}}}}
}

func rollingUpdateHostWithAncestor() *api.Host {
	const (
		clusterName = "default"
		shardName   = "0"
		hostName    = "r0"
	)
	host := &api.Host{Name: hostName}
	// A known version keeps this host out of the crash-recovery check, which
	// only applies when the version is still unknown.
	host.Runtime.Version = swversion.NewSoftWareVersion("25.3.1")
	host.Runtime.Address.ClusterName = clusterName
	host.Runtime.Address.ShardName = shardName
	host.Runtime.Address.HostName = hostName

	ancestorHost := &api.Host{Name: hostName}
	ancestorHost.Runtime.Address.HostName = hostName
	ancestor := &api.ClickHouseInstallation{
		Spec: api.ChiSpec{
			Configuration: &api.Configuration{
				Clusters: []*api.Cluster{{
					Name: clusterName,
					Layout: &api.ChiClusterLayout{
						Shards: []*api.ChiShard{{Name: shardName, Hosts: []*api.Host{ancestorHost}}},
					},
				}},
			},
		},
	}
	ancestorHost.Runtime.SetCR(ancestor)

	current := &api.ClickHouseInstallation{
		Spec: api.ChiSpec{
			Restart: types.NewString(api.RestartRollingUpdate),
			Configuration: &api.Configuration{
				Clusters: []*api.Cluster{{
					Name: clusterName,
					Layout: &api.ChiClusterLayout{
						Shards: []*api.ChiShard{{Name: shardName, Hosts: []*api.Host{host}}},
					},
				}},
			},
		},
	}
	current.EnsureStatus().NormalizedCRCompleted = ancestor
	host.Runtime.SetCR(current)
	return host
}
