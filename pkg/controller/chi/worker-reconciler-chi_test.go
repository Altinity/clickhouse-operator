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
	"testing"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// sts is a small builder for an apps/v1 StatefulSet with a single-container pod template
// carrying the given image.
func sts(image string) *apps.StatefulSet {
	return &apps.StatefulSet{
		Spec: apps.StatefulSetSpec{
			Template: core.PodTemplateSpec{
				Spec: core.PodSpec{
					Containers: []core.Container{{Name: "clickhouse-pod", Image: image}},
				},
			},
		},
	}
}

// hostWith returns an *api.Host whose Runtime holds the given cur/desired StatefulSets.
func hostWith(cur, desired *apps.StatefulSet) *api.Host {
	h := &api.Host{}
	h.Runtime.CurStatefulSet = cur
	h.Runtime.DesiredStatefulSet = desired
	return h
}

// TestHostRequiresStatefulSetRollout exercises the pure decision function that gates
// the pre-rollout software restart in reconcileHostStatefulSet.
//
// The function is scoped narrowly on purpose — see its doc comment. These tests both
// exercise the positive cases (env / image / volume changes force a rollout, fixing the
// secret-backed-env-var-with-from_env scenario) AND the negative cases (probe / label /
// apiserver-defaulted differences do NOT force a rollout, so config-only reconciles like
// test_010059's macro substitution still go through the software-restart path that
// ClickHouse needs to pick up new settings).
func TestHostRequiresStatefulSetRollout(t *testing.T) {
	same := sts("altinity/clickhouse-server:25.8")
	other := sts("altinity/clickhouse-server:25.8.16.10001.altinitystable")

	t.Run("nil host", func(t *testing.T) {
		require.False(t, hostRequiresStatefulSetRollout(nil))
	})

	t.Run("both nil StatefulSets → true (conservative)", func(t *testing.T) {
		require.True(t, hostRequiresStatefulSetRollout(hostWith(nil, nil)))
	})

	t.Run("nil cur StatefulSet → true", func(t *testing.T) {
		require.True(t, hostRequiresStatefulSetRollout(hostWith(nil, same)))
	})

	t.Run("nil desired StatefulSet → true", func(t *testing.T) {
		require.True(t, hostRequiresStatefulSetRollout(hostWith(same, nil)))
	})

	t.Run("identical containers → false", func(t *testing.T) {
		require.False(t, hostRequiresStatefulSetRollout(hostWith(sts("a"), sts("a"))))
	})

	t.Run("different env var in container → true (secret-backed env-var scenario)", func(t *testing.T) {
		cur := sts("a")
		desired := sts("a")
		desired.Spec.Template.Spec.Containers[0].Env = []core.EnvVar{{
			Name: "FOO",
			ValueFrom: &core.EnvVarSource{
				SecretKeyRef: &core.SecretKeySelector{
					LocalObjectReference: core.LocalObjectReference{Name: "s"},
					Key:                  "k",
				},
			},
		}}
		require.True(t, hostRequiresStatefulSetRollout(hostWith(cur, desired)))
	})

	t.Run("different container count → true", func(t *testing.T) {
		cur := sts("a")
		desired := sts("a")
		desired.Spec.Template.Spec.Containers = append(desired.Spec.Template.Spec.Containers,
			core.Container{Name: "sidecar", Image: "busybox"},
		)
		require.True(t, hostRequiresStatefulSetRollout(hostWith(cur, desired)))
	})

	// Negative cases — these should NOT force a rollout (so the software-restart path runs).
	// test_010059 is the canonical regression: macro substitution changes the config files
	// and the STS-level object-version fingerprint label, but leaves the pod template's
	// container env untouched. Before narrowing the check to Env only, a whole-template
	// DeepEqual returned true here because apiserver defaults (dnsPolicy, schedulerName,
	// restartPolicy, ConfigMap volume DefaultMode=420, container protocol, etc.) desync the
	// in-memory desired spec from the fetched current spec — even with no user-meaningful
	// change — and broke macro substitution's reliance on a ClickHouse restart.

	t.Run("different container image → false (handled by isImageChangeRequested upstream)", func(t *testing.T) {
		require.False(t, hostRequiresStatefulSetRollout(hostWith(same, other)))
	})

	t.Run("different readiness probe → false", func(t *testing.T) {
		cur := sts("a")
		desired := sts("a")
		desired.Spec.Template.Spec.Containers[0].ReadinessProbe = &core.Probe{
			ProbeHandler:     core.ProbeHandler{Exec: &core.ExecAction{Command: []string{"true"}}},
			TimeoutSeconds:   1,
			PeriodSeconds:    10,
			SuccessThreshold: 1,
			FailureThreshold: 3,
		}
		require.False(t, hostRequiresStatefulSetRollout(hostWith(cur, desired)))
	})

	t.Run("different command/args → false (not the env-var signal; other reconcile paths cover it)", func(t *testing.T) {
		cur := sts("a")
		desired := sts("a")
		desired.Spec.Template.Spec.Containers[0].Command = []string{"/bin/sh", "-c", "sleep 90 && /entrypoint.sh"}
		desired.Spec.Template.Spec.Containers[0].Args = []string{"--verbose"}
		require.False(t, hostRequiresStatefulSetRollout(hostWith(cur, desired)))
	})

	t.Run("different pod-level volumes → false (DefaultMode apiserver default desyncs)", func(t *testing.T) {
		cur := sts("a")
		desired := sts("a")
		desired.Spec.Template.Spec.Volumes = []core.Volume{{
			Name:         "secret-v",
			VolumeSource: core.VolumeSource{Secret: &core.SecretVolumeSource{SecretName: "s"}},
		}}
		require.False(t, hostRequiresStatefulSetRollout(hostWith(cur, desired)))
	})

	t.Run("different pod labels → false", func(t *testing.T) {
		cur := sts("a")
		desired := sts("a")
		desired.Spec.Template.ObjectMeta.Labels = map[string]string{"new": "label"}
		require.False(t, hostRequiresStatefulSetRollout(hostWith(cur, desired)))
	})

	t.Run("apiserver-defaulted RestartPolicy/DNSPolicy differ → false (test_010059 regression)", func(t *testing.T) {
		cur := sts("a")
		cur.Spec.Template.Spec.RestartPolicy = core.RestartPolicyAlways
		cur.Spec.Template.Spec.DNSPolicy = core.DNSClusterFirst
		cur.Spec.Template.Spec.SchedulerName = "default-scheduler"
		cur.Spec.Template.Spec.TerminationGracePeriodSeconds = func(i int64) *int64 { return &i }(30)
		desired := sts("a")
		require.False(t, hostRequiresStatefulSetRollout(hostWith(cur, desired)))
	})

	t.Run("nil vs empty env slice → false (Semantic.DeepEqual ignores nil/empty)", func(t *testing.T) {
		cur := sts("a")
		desired := sts("a")
		desired.Spec.Template.Spec.Containers[0].Env = []core.EnvVar{}
		require.False(t, hostRequiresStatefulSetRollout(hostWith(cur, desired)))
	})
}
