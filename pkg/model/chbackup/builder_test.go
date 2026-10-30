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

package chbackup

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

const testImage = "registry/clickhouse-backup:test"

func testTopology() *Topology {
	main := topologyCluster{
		name: "main",
		pod:  podSettings{image: testImage},
		shards: []topologyShard{
			{name: "0", hosts: []string{"chi-demo-main-0-0", "chi-demo-main-0-1"}},
			{name: "1", hosts: []string{"chi-demo-main-1-0", "chi-demo-main-1-1"}},
		},
	}
	return &Topology{installation: "demo", namespace: testNamespace, clusters: []topologyCluster{main}, enabled: []topologyCluster{main}}
}

func testInstallation() *api.ClickHouseInstallation {
	return newCHI("demo")
}

func testScheme(t *testing.T) *apiMachinery.Scheme {
	t.Helper()
	scheme := apiMachinery.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	return scheme
}

func testBackup(name string) *api.ClickHouseBackup {
	chb := &api.ClickHouseBackup{}
	chb.Name = name
	chb.Namespace = testNamespace
	chb.CreationTimestamp = meta.NewTime(time.Unix(1700000000, 0))
	chb.Spec.ClickHouseInstallation = "demo"
	return chb
}

func testRestore(name string) *api.ClickHouseRestore {
	chr := &api.ClickHouseRestore{}
	chr.Name = name
	chr.Namespace = testNamespace
	chr.Spec.ClickHouseInstallation = "demo"
	chr.Spec.BackupName = "nightly-20260101-000000"
	return chr
}

func testSchedule(name string) *api.ClickHouseBackupSchedule {
	chbs := &api.ClickHouseBackupSchedule{}
	chbs.Name = name
	chbs.Namespace = testNamespace
	chbs.Spec.ClickHouseInstallation = "demo"
	chbs.Spec.Schedule = "0 3 * * *"
	return chbs
}

func container(t *testing.T, spec core.PodSpec) core.Container {
	t.Helper()
	require.Len(t, spec.Containers, 1)
	return spec.Containers[0]
}

// env returns the plain values of a container's environment; values from a reference map to "".
func env(c core.Container) map[string]string {
	values := map[string]string{}
	for _, e := range c.Env {
		values[e.Name] = e.Value
	}
	return values
}

func envVarNamed(t *testing.T, c core.Container, name string) core.EnvVar {
	t.Helper()
	for _, e := range c.Env {
		if e.Name == name {
			return e
		}
	}
	require.Failf(t, "missing env var", "%s", name)
	return core.EnvVar{}
}

func TestBackupJobFollowsTheScriptContract(t *testing.T) {
	chb := testBackup("b1")
	chb.Spec.BackupName = "manual-1"
	chb.Spec.DiffFromRemote = "manual-0"
	chb.Spec.Tables = "db.*"
	chb.Spec.Partitions = []string{"202601", "202602"}
	chb.Spec.SchemaOnly = types.NewStringBool(true)

	job := BuildBackupJob(chb, testInstallation(), testTopology())
	c := container(t, job.Spec.Template.Spec)
	values := env(c)

	require.Equal(t, testImage, c.Image, "the Job runs the target cluster's clickhouse-backup image")
	require.Equal(t, operationBackup, values[envOperation])
	require.Equal(t, "main", values[envCluster])
	require.Equal(t, strconv.Itoa(config.ClickHouseBackupAPIPortNumber), values[envAPIPort])
	require.Equal(t, "5", values[envPollInterval])
	require.Equal(t, "manual-1", values[envBackupName])
	require.Equal(t, "manual-0", values[envDiffFromRemote])
	require.Equal(t, "chi-demo-main-0-0 chi-demo-main-1-0", values[envBackupHosts], "first replica per shard by default")
	require.Equal(t, "true", values[envSchemaOnly])
	require.Equal(t, "db.*", values[envTables])
	require.Equal(t, "202601,202602", values[envPartitions])
	require.Equal(t, NoDataMessage, values[envNoDataMessage])
	require.NotContains(t, values, envBackupNamePrefix, "a one-off backup has no retention")
	require.NotContains(t, values, envKeepLast)

	chb.Spec.ReplicaSelection = api.ReplicaSelectionAllReplicas
	values = env(container(t, BuildBackupJob(chb, testInstallation(), testTopology()).Spec.Template.Spec))
	require.Equal(t, "chi-demo-main-0-0 chi-demo-main-0-1 chi-demo-main-1-0 chi-demo-main-1-1", values[envBackupHosts])
}

// The REST API credentials stay in the operator-generated Secret: the Job references them and
// never carries them.
func TestJobsReferenceTheBackupSecret(t *testing.T) {
	secret := managers.NewNameManager(managers.NameManagerTypeClickHouse).Name(interfaces.NameBackupAutoSecret, testInstallation())

	for name, job := range map[string]*batch.Job{
		"backup":  BuildBackupJob(testBackup("b1"), testInstallation(), testTopology()),
		"verify":  BuildVerifyJob(testBackup("b1"), testInstallation(), testTopology()),
		"restore": BuildRestoreJob(testRestore("r1"), testInstallation(), testTopology()),
		"cronjob": {Spec: BuildBackupCronJob(testSchedule("s1"), testInstallation(), testTopology(), false).Spec.JobTemplate.Spec},
	} {
		t.Run(name, func(t *testing.T) {
			c := container(t, job.Spec.Template.Spec)
			for variable, key := range map[string]string{
				envAPIUsername: config.ClickHouseBackupSecretKeyAPIUsername,
				envAPIPassword: config.ClickHouseBackupSecretKeyAPIPassword,
			} {
				e := envVarNamed(t, c, variable)
				require.Empty(t, e.Value)
				require.NotNil(t, e.ValueFrom)
				require.NotNil(t, e.ValueFrom.SecretKeyRef)
				require.Equal(t, secret, e.ValueFrom.SecretKeyRef.Name)
				require.Equal(t, key, e.ValueFrom.SecretKeyRef.Key)
			}
		})
	}
}

// Every user-provided value reaches the script through the environment. The command is the
// static script text, whatever the spec holds - even a value validation would have refused.
func TestNoUserValueReachesTheCommand(t *testing.T) {
	hostile := "x'; rm -rf / #$(id)"

	chb := testBackup("b1")
	chb.Spec.BackupName = hostile
	chb.Spec.Tables = hostile
	chb.Spec.Partitions = []string{hostile}
	chb.Spec.DiffFromRemote = hostile
	chr := testRestore("r1")
	chr.Spec.BackupName = hostile
	chbs := testSchedule("s1")
	chbs.Spec.BackupNamePrefix = hostile

	for name, spec := range map[string]core.PodSpec{
		"backup":  BuildBackupJob(chb, testInstallation(), testTopology()).Spec.Template.Spec,
		"verify":  BuildVerifyJob(chb, testInstallation(), testTopology()).Spec.Template.Spec,
		"restore": BuildRestoreJob(chr, testInstallation(), testTopology()).Spec.Template.Spec,
		"cronjob": BuildBackupCronJob(chbs, testInstallation(), testTopology(), false).Spec.JobTemplate.Spec.Template.Spec,
	} {
		t.Run(name, func(t *testing.T) {
			c := container(t, spec)
			require.Equal(t, []string{"bash", "-c", jobScript}, receivedCommand(c.Command, nil), "kubelet hands the script over as written")
			require.Empty(t, c.Args)
			require.NotContains(t, strings.Join(c.Command, " "), hostile)
		})
	}
}

func TestJobPodIsRestricted(t *testing.T) {
	chb := testBackup("b1")
	for name, job := range map[string]*batch.Job{
		"backup":  BuildBackupJob(chb, testInstallation(), testTopology()),
		"restore": BuildRestoreJob(testRestore("r1"), testInstallation(), testTopology()),
		"cronjob": {Spec: BuildBackupCronJob(testSchedule("s1"), testInstallation(), testTopology(), false).Spec.JobTemplate.Spec},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, int32(0), ptr.Deref(job.Spec.BackoffLimit, -1), "a failed operation is never retried")

			spec := job.Spec.Template.Spec
			require.Equal(t, core.RestartPolicyNever, spec.RestartPolicy)
			require.False(t, ptr.Deref(spec.AutomountServiceAccountToken, true))
			require.False(t, ptr.Deref(spec.EnableServiceLinks, true),
				"service links could inject a variable such as API_PORT from a Service named api")

			require.NotNil(t, spec.SecurityContext)

			c := container(t, spec)
			require.False(t, ptr.Deref(c.SecurityContext.AllowPrivilegeEscalation, true))
			require.Equal(t, []core.Capability{"ALL"}, c.SecurityContext.Capabilities.Drop)
			require.GreaterOrEqual(t, c.Resources.Limits.Memory().Cmp(resource.MustParse("512Mi")), 0,
				"the restore preflight holds the table lists of all target hosts in memory")
		})
	}
}

// A namespace whose ResourceQuota covers limits admits no pod without them unless a LimitRange
// provides defaults, and a LimitRange may cap the ratio of a limit to its request.
func TestJobPodFitsQuotasAndLimitRanges(t *testing.T) {
	const maxLimitRequestRatio = 10
	resources := container(t, BuildBackupJob(testBackup("b1"), testInstallation(), testTopology()).Spec.Template.Spec).Resources
	for _, name := range []core.ResourceName{core.ResourceCPU, core.ResourceMemory} {
		request, requested := resources.Requests[name]
		limit, limited := resources.Limits[name]
		require.True(t, requested, "%s request", name)
		require.True(t, limited, "%s limit", name)
		require.False(t, request.IsZero(), "%s request", name)
		require.LessOrEqual(t, float64(limit.MilliValue())/float64(request.MilliValue()), float64(maxLimitRequestRatio), "%s limit to request ratio", name)
	}
}

// A Job runs with what admission already accepts for the installation's own pods: the pod
// security context, registry credentials and service account of the hosts' pod template, and on its
// container the identity and the seccomp profile of the hosts' sidecar, which take precedence over
// the pod's in the Job as they do in the sidecar. Nothing is invented: OpenShift's restricted SCC
// assigns the user from a namespace range and refuses any other, and an SCC without seccomp profiles
// refuses any profile.
func TestJobPodInheritsThePodTemplate(t *testing.T) {
	localhost := &core.SeccompProfile{Type: core.SeccompProfileTypeLocalhost, LocalhostProfile: ptr.To("profiles/backup.json")}
	runtimeDefault := &core.SeccompProfile{Type: core.SeccompProfileTypeRuntimeDefault}
	for name, tc := range map[string]struct {
		inherited *core.PodSecurityContext
		sidecar   *core.SecurityContext
		expected  *core.PodSecurityContext
		// expectedContainer is the identity and seccomp profile of the Job container
		expectedContainer core.SecurityContext
	}{
		"no user, as an SCC assigns it": {
			inherited: &core.PodSecurityContext{},
			expected:  &core.PodSecurityContext{},
		},
		"explicit user": {
			inherited: &core.PodSecurityContext{RunAsUser: ptr.To(int64(101)), FSGroup: ptr.To(int64(101))},
			expected:  &core.PodSecurityContext{RunAsUser: ptr.To(int64(101)), RunAsNonRoot: ptr.To(true), FSGroup: ptr.To(int64(101))},
		},
		"explicit settings are kept": {
			inherited: &core.PodSecurityContext{RunAsUser: ptr.To(int64(0)), RunAsNonRoot: ptr.To(false), SeccompProfile: localhost},
			expected:  &core.PodSecurityContext{RunAsUser: ptr.To(int64(0)), RunAsNonRoot: ptr.To(false), SeccompProfile: localhost},
		},
		"the identity and seccomp profile of the sidecar": {
			inherited:         &core.PodSecurityContext{FSGroup: ptr.To(int64(101))},
			sidecar:           &core.SecurityContext{RunAsUser: ptr.To(int64(101)), RunAsGroup: ptr.To(int64(101)), RunAsNonRoot: ptr.To(true), SeccompProfile: runtimeDefault},
			expected:          &core.PodSecurityContext{FSGroup: ptr.To(int64(101))},
			expectedContainer: core.SecurityContext{RunAsUser: ptr.To(int64(101)), RunAsGroup: ptr.To(int64(101)), RunAsNonRoot: ptr.To(true), SeccompProfile: runtimeDefault},
		},
		"the user of the sidecar without a pod security context": {
			sidecar:           &core.SecurityContext{RunAsUser: ptr.To(int64(101))},
			expected:          &core.PodSecurityContext{},
			expectedContainer: core.SecurityContext{RunAsUser: ptr.To(int64(101)), RunAsNonRoot: ptr.To(true)},
		},
		"the sidecar's user over the pod's": {
			inherited:         &core.PodSecurityContext{RunAsUser: ptr.To(int64(1000))},
			sidecar:           &core.SecurityContext{RunAsUser: ptr.To(int64(101)), RunAsGroup: ptr.To(int64(101))},
			expected:          &core.PodSecurityContext{RunAsUser: ptr.To(int64(1000)), RunAsNonRoot: ptr.To(true)},
			expectedContainer: core.SecurityContext{RunAsUser: ptr.To(int64(101)), RunAsGroup: ptr.To(int64(101)), RunAsNonRoot: ptr.To(true)},
		},
		"the sidecar's seccomp profile over the pod's": {
			inherited:         &core.PodSecurityContext{SeccompProfile: localhost},
			sidecar:           &core.SecurityContext{SeccompProfile: runtimeDefault},
			expected:          &core.PodSecurityContext{SeccompProfile: localhost},
			expectedContainer: core.SecurityContext{SeccompProfile: runtimeDefault},
		},
		"non-root required by the sidecar without a user": {
			inherited:         &core.PodSecurityContext{FSGroup: ptr.To(int64(101))},
			sidecar:           &core.SecurityContext{RunAsNonRoot: ptr.To(true)},
			expected:          &core.PodSecurityContext{FSGroup: ptr.To(int64(101))},
			expectedContainer: core.SecurityContext{RunAsNonRoot: ptr.To(true)},
		},
		"non-root required by the pod without a user": {
			inherited: &core.PodSecurityContext{RunAsNonRoot: ptr.To(true), RunAsGroup: ptr.To(int64(2000))},
			expected:  &core.PodSecurityContext{RunAsNonRoot: ptr.To(true), RunAsGroup: ptr.To(int64(2000))},
		},
		// Root is no identity to take over: the Job needs no privilege.
		"a root sidecar": {
			sidecar:  &core.SecurityContext{RunAsUser: ptr.To(int64(0)), RunAsGroup: ptr.To(int64(0)), RunAsNonRoot: ptr.To(false)},
			expected: &core.PodSecurityContext{},
		},
		"sysctls stay with the hosts' nodes": {
			inherited: &core.PodSecurityContext{RunAsUser: ptr.To(int64(101)), Sysctls: []core.Sysctl{{Name: "net.core.somaxconn", Value: "4096"}}},
			expected:  &core.PodSecurityContext{RunAsUser: ptr.To(int64(101)), RunAsNonRoot: ptr.To(true)},
		},
		"no identity in the sidecar": {
			sidecar:  &core.SecurityContext{AllowPrivilegeEscalation: ptr.To(true), Privileged: ptr.To(true)},
			expected: &core.PodSecurityContext{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			topology := testTopology()
			topology.clusters[0].pod = podSettings{
				image:                    testImage,
				securityContext:          tc.inherited,
				containerSecurityContext: tc.sidecar,
				imagePullSecrets:         []core.LocalObjectReference{{Name: "registry"}},
				serviceAccountName:       "clickhouse",
			}
			before, beforeSidecar := tc.inherited.DeepCopy(), tc.sidecar.DeepCopy()
			expectedContainer := tc.expectedContainer.DeepCopy()
			// The Job container never takes privileges from the sidecar
			expectedContainer.AllowPrivilegeEscalation = ptr.To(false)
			expectedContainer.Capabilities = &core.Capabilities{Drop: []core.Capability{capabilityAll}}

			for kind, spec := range map[string]core.PodSpec{
				"backup":  BuildBackupJob(testBackup("b1"), testInstallation(), topology).Spec.Template.Spec,
				"verify":  BuildVerifyJob(testBackup("b1"), testInstallation(), topology).Spec.Template.Spec,
				"restore": BuildRestoreJob(testRestore("r1"), testInstallation(), topology).Spec.Template.Spec,
				"cronjob": BuildBackupCronJob(testSchedule("s1"), testInstallation(), topology, false).Spec.JobTemplate.Spec.Template.Spec,
			} {
				require.Equal(t, tc.expected, spec.SecurityContext, kind)
				require.Equal(t, expectedContainer, spec.Containers[0].SecurityContext, kind)
				require.Equal(t, []core.LocalObjectReference{{Name: "registry"}}, spec.ImagePullSecrets, kind)
				require.Equal(t, "clickhouse", spec.ServiceAccountName, kind)
				require.False(t, ptr.Deref(spec.AutomountServiceAccountToken, true), "%s: the Job needs no API access", kind)
				for _, seccomp := range []*core.SeccompProfile{spec.SecurityContext.SeccompProfile, spec.Containers[0].SecurityContext.SeccompProfile} {
					if seccomp != nil {
						*seccomp = core.SeccompProfile{}
					}
				}
				for _, user := range []*int64{spec.SecurityContext.RunAsUser, spec.Containers[0].SecurityContext.RunAsUser} {
					if user != nil {
						*user = -1
					}
				}
			}
			require.Equal(t, before, tc.inherited, "the topology is shared by every Job built from it")
			require.Equal(t, beforeSidecar, tc.sidecar, "the topology is shared by every Job built from it")
		})
	}
}

// A pod counts as started once its Job container ran, whatever runs next to it.
func TestJobContainerStarted(t *testing.T) {
	jobContainer := func(state, last core.ContainerState) core.ContainerStatus {
		return core.ContainerStatus{Name: jobContainerName, State: state, LastTerminationState: last}
	}
	waiting := core.ContainerState{Waiting: &core.ContainerStateWaiting{Reason: "ImagePullBackOff"}}
	running := core.ContainerState{Running: &core.ContainerStateRunning{}}
	terminated := core.ContainerState{Terminated: &core.ContainerStateTerminated{}}
	sidecar := core.ContainerStatus{Name: "istio-proxy", State: running}
	for name, tc := range map[string]struct {
		status  core.PodStatus
		started bool
	}{
		"pending":                         {status: core.PodStatus{Phase: core.PodPending}},
		"image not pulled":                {status: core.PodStatus{Phase: core.PodPending, ContainerStatuses: []core.ContainerStatus{jobContainer(waiting, core.ContainerState{})}}},
		"only an injected sidecar runs":   {status: core.PodStatus{Phase: core.PodPending, ContainerStatuses: []core.ContainerStatus{sidecar, jobContainer(waiting, core.ContainerState{})}}},
		"running":                         {status: core.PodStatus{Phase: core.PodRunning}, started: true},
		"succeeded":                       {status: core.PodStatus{Phase: core.PodSucceeded}, started: true},
		"failed":                          {status: core.PodStatus{Phase: core.PodFailed}, started: true},
		"Job container runs":              {status: core.PodStatus{Phase: core.PodPending, ContainerStatuses: []core.ContainerStatus{jobContainer(running, core.ContainerState{})}}, started: true},
		"Job container ended":             {status: core.PodStatus{Phase: core.PodPending, ContainerStatuses: []core.ContainerStatus{jobContainer(terminated, core.ContainerState{})}}, started: true},
		"Job container ran and now waits": {status: core.PodStatus{Phase: core.PodPending, ContainerStatuses: []core.ContainerStatus{jobContainer(waiting, terminated)}}, started: true},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.started, JobContainerStarted(&core.Pod{Status: tc.status}))
		})
	}
}

func TestJobDeadlineComesFromTheSpec(t *testing.T) {
	chb := testBackup("b1")
	require.Equal(t, int64(24*60*60), ptr.Deref(BuildBackupJob(chb, testInstallation(), testTopology()).Spec.ActiveDeadlineSeconds, 0))
	chb.Spec.ActiveDeadlineSeconds = ptr.To(int64(600))
	require.Equal(t, int64(600), ptr.Deref(BuildBackupJob(chb, testInstallation(), testTopology()).Spec.ActiveDeadlineSeconds, 0))
	require.Equal(t, int64(600), ptr.Deref(BuildVerifyJob(chb, testInstallation(), testTopology()).Spec.ActiveDeadlineSeconds, 0))

	chr := testRestore("r1")
	chr.Spec.ActiveDeadlineSeconds = ptr.To(int64(900))
	require.Equal(t, int64(900), ptr.Deref(BuildRestoreJob(chr, testInstallation(), testTopology()).Spec.ActiveDeadlineSeconds, 0))

	chbs := testSchedule("s1")
	chbs.Spec.ActiveDeadlineSeconds = ptr.To(int64(1200))
	cronJob := BuildBackupCronJob(chbs, testInstallation(), testTopology(), false)
	require.Equal(t, int64(1200), ptr.Deref(cronJob.Spec.JobTemplate.Spec.ActiveDeadlineSeconds, 0))
}

// A restore creates the schema on every replica of its cluster from the backup's own definitions, so
// that no Distributed table is rewritten to point at the cluster, and restores the data on the first
// replica of each shard, from which replication fills the others.
func TestRestoreJobFollowsTheScriptContract(t *testing.T) {
	chr := testRestore("r1")
	values := env(container(t, BuildRestoreJob(chr, testInstallation(), testTopology()).Spec.Template.Spec))

	require.Equal(t, operationRestore, values[envOperation])
	require.Equal(t, "main", values[envCluster])
	require.Equal(t, chr.Spec.BackupName, values[envBackupName])
	require.Equal(t, "chi-demo-main-0-0 chi-demo-main-0-1 chi-demo-main-1-0 chi-demo-main-1-1", values[envSchemaHosts],
		"the schema is restored on every replica")
	require.Equal(t, "chi-demo-main-0-0 chi-demo-main-1-0", values[envDataHosts])
	require.Equal(t, "chi-demo-main-0-0 chi-demo-main-0-1 chi-demo-main-1-0 chi-demo-main-1-1", values[envGuardHosts],
		"the overwrite guard checks every replica, as the schema restore drops the tables on each")
	require.Equal(t, "false", values[envSchemaOnly])
	require.Empty(t, values[envTables])
	require.Equal(t, "false", values[envOverwrite])
	require.Equal(t, "true", values[envValidateTopology], "topology validation is on unless switched off")
	require.Equal(t, "false", values[envRestoreRBAC])
	require.Equal(t, RBACNotRestoredMessage, values[envRBACNotRestoredMessage])
	require.NotContains(t, values, envPathReplicaSelection, "a restore checks every replica, whatever a backup was made on")

	chr.Spec.Overwrite = types.NewStringBool(true)
	chr.Spec.SchemaOnly = types.NewStringBool(true)
	chr.Spec.ValidateTopology = types.NewStringBool(false)
	chr.Spec.Tables = "db.*"
	values = env(container(t, BuildRestoreJob(chr, testInstallation(), testTopology()).Spec.Template.Spec))
	require.Equal(t, "true", values[envOverwrite])
	require.Equal(t, "true", values[envSchemaOnly])
	require.Equal(t, "false", values[envValidateTopology])
	require.Equal(t, "db.*", values[envTables])
}

// Access entities belong to the whole installation: a restore restores them only when it targets
// every cluster, or when the operator recovers a cluster of a new installation, which owns the
// recovery restore.
func TestRestoreJobRestoresRBACOnlyWhereAllowed(t *testing.T) {
	chi := testInstallation()
	chi.UID = "chi-uid"
	recovery := BuildRecoveryRestore(chi, ClusterRecovery{Cluster: "main", BackupName: "nightly-20260101-000000"})
	require.NoError(t, controllerutil.SetControllerReference(chi, recovery, testScheme(t)))
	foreign := recovery.DeepCopy()
	foreign.OwnerReferences[0].Name = "another"

	for name, tc := range map[string]struct {
		chr  func() *api.ClickHouseRestore
		rbac bool
	}{
		"not asked": {chr: func() *api.ClickHouseRestore { return testRestore("r1") }},
		"every cluster": {chr: func() *api.ClickHouseRestore {
			chr := testRestore("r1")
			chr.Spec.RBAC = types.NewStringBool(true)
			return chr
		}, rbac: true},
		"one cluster": {chr: func() *api.ClickHouseRestore {
			chr := testRestore("r1")
			chr.Spec.RBAC = types.NewStringBool(true)
			chr.Spec.Cluster = "main"
			return chr
		}},
		"the recovery of a cluster":           {chr: func() *api.ClickHouseRestore { return recovery }, rbac: true},
		"a restore another installation owns": {chr: func() *api.ClickHouseRestore { return foreign }},
	} {
		t.Run(name, func(t *testing.T) {
			chr := tc.chr()
			require.Equal(t, tc.rbac, RestoresRBAC(chr))
			values := env(container(t, BuildRestoreJob(chr, chi, testTopology()).Spec.Template.Spec))
			require.Equal(t, strconv.FormatBool(tc.rbac), values[envRestoreRBAC])
		})
	}
}

func TestVerifyJobFollowsTheScriptContract(t *testing.T) {
	chb := testBackup("b1")
	values := env(container(t, BuildVerifyJob(chb, testInstallation(), testTopology()).Spec.Template.Spec))

	require.Equal(t, operationVerify, values[envOperation])
	require.Equal(t, "main", values[envCluster])
	require.Equal(t, ResolveBackupName(chb), values[envBackupName])
	require.Equal(t, "chi-demo-main-0-0 chi-demo-main-1-0", values[envVerifyHosts])
}

func TestBackupCronJobFollowsTheScriptContract(t *testing.T) {
	chbs := testSchedule("nightly")
	chbs.Spec.Retention.KeepLast = ptr.To(int32(7))
	chbs.Spec.SuccessfulJobsHistoryLimit = ptr.To(int32(5))

	cronJob := BuildBackupCronJob(chbs, testInstallation(), testTopology(), false)
	values := env(container(t, cronJob.Spec.JobTemplate.Spec.Template.Spec))

	require.Equal(t, "ns-demo-nightly", values[envBackupNamePrefix],
		"the prefix defaults to the namespace, installation and schedule names, the same on every cluster")
	require.Equal(t, "nightly-main-backup", cronJob.Name)
	require.Equal(t, "nightly-main-backup", values[envCronJobName])
	require.Equal(t, "main", values[envCluster])
	jobName := envVarNamed(t, container(t, cronJob.Spec.JobTemplate.Spec.Template.Spec), envJobName)
	require.Equal(t, "metadata.labels['job-name']", jobName.ValueFrom.FieldRef.FieldPath,
		"a run takes the name of its Job from its pod, by the label every supported Kubernetes version gives it")
	require.NotContains(t, values, envBackupName, "every run names its backup at runtime")
	require.Equal(t, "7", values[envKeepLast])
	require.Equal(t, values[envBackupHosts], values[envRetentionHosts], "retention prunes on every host the backup ran on")
	require.Equal(t, NoDataMessage, values[envNoDataMessage])
	require.Equal(t, "0 3 * * *", cronJob.Spec.Schedule)
	require.Equal(t, batch.ForbidConcurrent, cronJob.Spec.ConcurrencyPolicy)
	require.Equal(t, int32(5), ptr.Deref(cronJob.Spec.SuccessfulJobsHistoryLimit, 0))
	require.False(t, ptr.Deref(cronJob.Spec.Suspend, true))
	require.Equal(t, cronJob.Labels, cronJob.Spec.JobTemplate.Labels, "scheduled Jobs must carry the labels the cache selects by")

	chbs.Spec.ReplicaSelection = api.ReplicaSelectionAllReplicas
	chbs.Spec.BackupNamePrefix = "custom"
	values = env(container(t, BuildBackupCronJob(chbs, testInstallation(), testTopology(), true).Spec.JobTemplate.Spec.Template.Spec))
	require.Equal(t, "custom", values[envBackupNamePrefix])
	require.Equal(t, "chi-demo-main-0-0 chi-demo-main-0-1 chi-demo-main-1-0 chi-demo-main-1-1", values[envRetentionHosts])
	require.Equal(t, values[envBackupHosts], values[envRetentionHosts])
	require.Equal(t, "0", env(container(t, BuildBackupCronJob(testSchedule("s"), testInstallation(), testTopology(), false).Spec.JobTemplate.Spec.Template.Spec))[envKeepLast])
}

// A schedule is read in the time zone it names, as the CronJob's, and in that of the Kubernetes controller manager
// without one. Backup names carry UTC times either way.
func TestBackupCronJobReadsTheScheduleInItsTimeZone(t *testing.T) {
	chbs := testSchedule("nightly")
	require.Nil(t, BuildBackupCronJob(chbs, testInstallation(), testTopology(), false).Spec.TimeZone)
	chbs.Spec.TimeZone = "Europe/Berlin"
	require.Equal(t, ptr.To("Europe/Berlin"), BuildBackupCronJob(chbs, testInstallation(), testTopology(), false).Spec.TimeZone)
}

// A backup that runs a schedule once is named with the schedule's prefix, which the recovery prefix guard tells,
// and the time it is due in a form the series the schedule's retention prunes never takes: creating one never
// pushes a backup of the schedule out. Its Jobs make it as the schedule's runs do, with the options it recorded.
func TestBackupThatRunsAScheduleStaysOutOfItsSeries(t *testing.T) {
	chbs := testSchedule("nightly")
	chbs.Spec.Tables = "db.*"
	chbs.Spec.ReplicaSelection = api.ReplicaSelectionAllReplicas
	chbs.Spec.ActiveDeadlineSeconds = ptr.To(int64(600))
	chb := testBackup("pre-upgrade")
	chb.Spec.ClickHouseInstallation = ""
	chb.Spec.ScheduleRef = &api.BackupScheduleRef{Name: chbs.GetName()}
	chb.Status.Schedule = &api.BackupScheduleRecord{
		Name:                   chbs.GetName(),
		ClickHouseInstallation: chbs.Spec.ClickHouseInstallation,
		BackupNamePrefix:       BackupNamePrefix(chbs),
		BackupOptions:          chbs.Spec.BackupOptions,
	}

	name := ResolveBackupName(chb)
	require.Equal(t, "ns-demo-nightly-20231114T221320", name, "due when created, at 1700000000")
	series := regexp.MustCompile("^" + regexp.QuoteMeta(BackupNamePrefix(chbs)) + "-[0-9]{8}-[0-9]{6}$")
	require.False(t, series.MatchString(name), "the retention of the schedule prunes only the backups of its runs")
	require.True(t, HasDefaultBackupNamePrefix(testNamespace, "demo", name))
	named := testBackup("named")
	named.Spec.BackupName = name
	require.NoError(t, named.Spec.Validate(), "a valid backup name, which no retention of a schedule prunes")

	chb.Spec.ScheduledTime = "2026-10-20T02:00:00+02:00"
	require.Equal(t, "ns-demo-nightly-20261020T000000", ResolveBackupName(chb), "due at its scheduled time, in UTC")
	chb.Status.Schedule.BackupNamePrefix = strings.Repeat("p", api.BackupNamePrefixMaxLength)
	require.Len(t, ResolveBackupName(chb), api.BackupNameMaxLength, "the name of the longest prefix fits")

	job := BuildBackupJob(chb, testInstallation(), testTopology())
	values := env(container(t, job.Spec.Template.Spec))
	require.Equal(t, "db.*", values[envTables])
	require.Equal(t, "chi-demo-main-0-0 chi-demo-main-0-1 chi-demo-main-1-0 chi-demo-main-1-1", values[envBackupHosts])
	require.Equal(t, int64(600), ptr.Deref(job.Spec.ActiveDeadlineSeconds, 0))
	require.Equal(t, LabelValue("demo"), job.Labels[LabelBackupCHI], "the Jobs are locked and held by the installation of the schedule")
	require.NotContains(t, values, envKeepLast, "only the runs of a schedule prune")
}

// Only a backup that runs a schedule is named with the prefix it recorded of it: where the API server does not
// enforce the immutable scheduleRef, a backup edited to name its installation is named as any other backup, by
// its explicit name or after its own name, whatever record of a schedule its status still holds.
func TestBackupNamedWithTheSchedulePrefixOnlyWithScheduleRef(t *testing.T) {
	chb := testBackup("b1")
	chb.Spec.BackupName = "explicit"
	chb.Status.Schedule = &api.BackupScheduleRecord{Name: "nightly", BackupNamePrefix: "ns-demo-nightly"}
	require.Equal(t, "explicit", ResolveBackupName(chb))
	chb.Spec.BackupName = ""
	require.Equal(t, "b1-1700000000", ResolveBackupName(chb))
}

// A plain backup due at a scheduled time is named after that time, as one due when created is after its creation.
func TestBackupDueLaterIsNamedAfterItsScheduledTime(t *testing.T) {
	chb := testBackup("b1")
	chb.Spec.ScheduledTime = "2026-10-20T02:00:00+02:00"
	require.Equal(t, "b1-1792454400", ResolveBackupName(chb))
}

// The CronJob is updated only when its fingerprint changes, so the fingerprint has to follow
// everything the reconcile can change - the hosts above all - and nothing else.
func TestBackupCronJobVersionFollowsItsSpec(t *testing.T) {
	version := func(cronJob *batch.CronJob) string {
		return cronJob.Annotations[AnnotationBackupObjectVersion]
	}
	chbs := testSchedule("nightly")

	base := BuildBackupCronJob(chbs, testInstallation(), testTopology(), false)
	require.NotEmpty(t, version(base))
	require.Equal(t, version(base), version(BuildBackupCronJob(chbs, testInstallation(), testTopology(), false)))

	scaled := testTopology()
	scaled.clusters[0].shards = append(scaled.clusters[0].shards, topologyShard{name: "2", hosts: []string{"chi-demo-main-2-0"}})
	require.NotEqual(t, version(base), version(BuildBackupCronJob(chbs, testInstallation(), scaled, false)))
	require.NotEqual(t, version(base), version(BuildBackupCronJob(chbs, testInstallation(), testTopology(), true)))
}

// Every Job and CronJob drives one cluster, which its name tells.
func TestNamesFitTheirLimits(t *testing.T) {
	require.Equal(t, "b1-main-backup", BackupJobName(testBackup("b1"), "main"), "short names stay readable")
	require.Equal(t, "b1-main-verify", VerifyJobName(testBackup("b1"), "main"))
	require.Equal(t, "r1-main-restore", RestoreJobName(testRestore("r1"), "main"))
	require.Equal(t, "s1-main-backup", CronJobName(testSchedule("s1"), "main"))
	require.Equal(t, "demo-main-recovery", RecoveryRestoreName(testInstallation(), "main"))
	require.NotEqual(t, BackupJobName(testBackup("b1"), "hot"), BackupJobName(testBackup("b1"), "cold"))

	long := strings.Repeat("a", 200) + ".b"
	longToo := strings.Repeat("a", 200) + ".c"
	const cluster = "fifteen-letters"

	for _, name := range []string{BackupJobName(testBackup(long), cluster), VerifyJobName(testBackup(long), cluster), RestoreJobName(testRestore(long), cluster)} {
		require.LessOrEqual(t, len(name), validation.LabelValueMaxLength, name)
		require.Empty(t, validation.IsDNS1123Subdomain(name), name)
		require.Empty(t, validation.IsValidLabelValue(name), "the Job controller labels pods with the Job name")
	}
	require.LessOrEqual(t, len(CronJobName(testSchedule(long), cluster)), 52, "the CronJob controller appends 11 characters")
	require.Empty(t, validation.IsDNS1123Subdomain(CronJobName(testSchedule(long), cluster)))
	chi := testInstallation()
	chi.Name = long
	require.Empty(t, validation.IsDNS1123Subdomain(RecoveryRestoreName(chi, cluster)))

	require.Equal(t, BackupJobName(testBackup(long), cluster), BackupJobName(testBackup(long), cluster), "names must be stable across reconciles")
	require.NotEqual(t, BackupJobName(testBackup(long), cluster), BackupJobName(testBackup(longToo), cluster), "shortened names must stay distinct")
	require.NotEqual(t, BackupJobName(testBackup(long), "hot"), BackupJobName(testBackup(long), "cold"), "shortened names must stay distinct")

	for _, value := range Labels(api.ClickHouseBackupCRDResourceKind, long, long, cluster, testStorage) {
		require.Empty(t, validation.IsValidLabelValue(value), value)
	}
}

func TestResolvedBackupNamesPassValidation(t *testing.T) {
	short := testBackup("b1")
	require.Equal(t, "b1-1700000000", ResolveBackupName(short))
	require.Equal(t, ResolveBackupName(short), ResolveBackupName(short.DeepCopy()), "the name must not change between reconciles")

	for _, chb := range []*api.ClickHouseBackup{short, testBackup(strings.Repeat("a", 250))} {
		named := chb.DeepCopy()
		named.Spec.BackupName = ResolveBackupName(chb)
		require.NoError(t, named.Spec.Validate(), named.Spec.BackupName)
	}

	chbs := testSchedule(strings.Repeat("s", 250))
	prefix := BackupNamePrefix(chbs)
	require.LessOrEqual(t, len(prefix)+api.BackupNameScheduleSuffixLength, api.BackupNameMaxLength, "a scheduled backup name must fit the backup name limit")
	chbs.Spec.BackupNamePrefix = prefix
	require.NoError(t, chbs.Spec.Validate())
}

// The suffix a scheduled run appends to its backup name prefix is spelled three times: the API
// bounds prefixes by its length, the operator derives it with scheduleTimestampLayout and the Job
// script with date. The longest prefix the API admits must still give a backup name it admits.
func TestScheduledBackupNameSuffixFitsTheBackupNameLimit(t *testing.T) {
	require.Equal(t, api.BackupNameScheduleSuffixLength, len("-"+scheduleTimestampLayout))

	chbs := testSchedule("nightly")
	chbs.Spec.BackupNamePrefix = strings.Repeat("p", api.BackupNamePrefixMaxLength)
	require.NoError(t, chbs.Spec.Validate())
	name := ScheduledBackupName(scheduledRun(chbs, time.Date(2026, time.January, 2, 3, 4, 0, 0, time.UTC)))
	require.Len(t, name, api.BackupNameMaxLength)

	chr := testRestore("r1")
	chr.Spec.BackupName = name
	require.NoError(t, chr.Spec.Validate(), "a restore must be able to name the backup a scheduled run made")
}

// Retention prunes by prefix. A restored installation typically shares the storage path of
// the installation its backup came from, often under the same name in another namespace, so a
// schedule of the same name on each must not get the same default prefix: each would prune the
// backups of the other.
func TestBackupNamePrefixDefaultsToNamespaceInstallationAndSchedule(t *testing.T) {
	source := testSchedule("nightly")
	clone := testSchedule("nightly")
	clone.Spec.ClickHouseInstallation = "demo-restored"
	sameName := testSchedule("nightly")
	sameName.Namespace = "staging"
	require.Equal(t, "ns-demo-nightly", BackupNamePrefix(source))
	require.Equal(t, "ns-demo-restored-nightly", BackupNamePrefix(clone))
	require.Equal(t, "staging-demo-nightly", BackupNamePrefix(sameName))

	long := testSchedule(strings.Repeat("s", 250))
	long.Spec.ClickHouseInstallation = strings.Repeat("c", 250)
	longToo := long.DeepCopy()
	longToo.Spec.ClickHouseInstallation = strings.Repeat("c", 249) + "d"
	for _, chbs := range []*api.ClickHouseBackupSchedule{source, clone, sameName, long, longToo} {
		prefix := BackupNamePrefix(chbs)
		require.LessOrEqual(t, len(prefix), api.BackupNamePrefixMaxLength, prefix)
		chbs.Spec.BackupNamePrefix = prefix
		require.NoError(t, chbs.Spec.Validate(), prefix)
		chbs.Spec.BackupNamePrefix = ""
	}
	require.Equal(t, BackupNamePrefix(long), BackupNamePrefix(long.DeepCopy()), "the prefix must not change between reconciles")
	require.NotEqual(t, BackupNamePrefix(long), BackupNamePrefix(longToo), "shortened prefixes must stay distinct")

	source.Spec.BackupNamePrefix = "custom"
	require.Equal(t, "custom", BackupNamePrefix(source))
}

// Every schedule of an installation that leaves its backup name prefix to the default names its backups
// after the namespace and the installation, whatever the schedule, shortened or not: a backup carries
// the default prefix of the installation exactly when it starts with what every such prefix starts
// with.
func TestHasDefaultBackupNamePrefix(t *testing.T) {
	for name, tc := range map[string]struct {
		namespace, chi string
	}{
		"short names":                          {namespace: testNamespace, chi: "demo"},
		"names a shortened prefix keeps whole": {namespace: testNamespace, chi: strings.Repeat("c", 90)},
		"names a shortened prefix cuts":        {namespace: testNamespace, chi: strings.Repeat("c", 120)},
		"names only a prefix left whole keeps": {namespace: testNamespace, chi: strings.Repeat("c", 104)},
	} {
		t.Run(name, func(t *testing.T) {
			for _, schedule := range []string{"s", "nightly", strings.Repeat("s", 30), strings.Repeat("s", 200)} {
				chbs := testSchedule(schedule)
				chbs.Namespace, chbs.Spec.ClickHouseInstallation = tc.namespace, tc.chi
				backup := BackupNamePrefix(chbs) + "-20261004-020000"
				require.True(t, HasDefaultBackupNamePrefix(tc.namespace, tc.chi, backup), backup)

				other := chbs.DeepCopy()
				other.Spec.ClickHouseInstallation = "x" + tc.chi
				require.False(t, HasDefaultBackupNamePrefix(tc.namespace, tc.chi, BackupNamePrefix(other)+"-20261004-020000"))
			}
			require.False(t, HasDefaultBackupNamePrefix(tc.namespace, tc.chi, "custom-20261004-020000"))
			// An installation whose name extends this one's is another one, the shortened prefix included
			installation := joinName(tc.namespace, tc.chi)
			for _, root := range []string{installation, boundedHead(installation, "", api.BackupNamePrefixMaxLength)} {
				require.False(t, HasDefaultBackupNamePrefix(tc.namespace, tc.chi, root+"2-nightly-20261004-020000"), root)
			}
		})
	}
}

// A restore keeps to the cluster recorded when it was validated, whatever its spec names later.
func TestRestoresRBACFollowsThePinnedCluster(t *testing.T) {
	chr := testRestore("r1")
	chr.Spec.RBAC = types.NewStringBool(true)
	require.True(t, RestoresRBAC(chr), "a restore of every cluster restores access entities")
	chr.Spec.Cluster = "main"
	require.False(t, RestoresRBAC(chr), "a restore of one cluster does not")

	chr.Status.Clusters = []api.OperationClusterStatus{{Name: "main"}}
	chr.Status.Cluster = ""
	require.True(t, RestoresRBAC(chr), "validated as a restore of every cluster, it stays one")
	chr.Spec.Cluster, chr.Status.Cluster = "", "main"
	require.False(t, RestoresRBAC(chr), "validated as a restore of one cluster, it stays one")
}

// A verification checks the backup that was made. The spec may name another one by now.
func TestVerifyJobVerifiesTheBackupThatWasMade(t *testing.T) {
	chb := testBackup("b1")
	chb.Spec.BackupName = "edited-later"
	chb.Status.BackupName = "made"
	job := BuildVerifyJob(chb, testInstallation(), testTopology())
	require.Equal(t, "made", env(container(t, job.Spec.Template.Spec))[envBackupName])
	require.Equal(t, "made", JobBackupName(job))
}

// What a Job works on is what it was built with, whatever its custom resource says by now.
func TestJobBackupName(t *testing.T) {
	chb := testBackup("b1")
	require.Equal(t, ResolveBackupName(chb), JobBackupName(BuildBackupJob(chb, testInstallation(), testTopology())))
	chr := testRestore("r1")
	job := BuildRestoreJob(chr, testInstallation(), testTopology())
	chr.Spec.BackupName = "edited-later"
	require.Equal(t, "nightly-20260101-000000", JobBackupName(job))

	run := scheduledRun(testSchedule("nightly"), time.Date(2026, time.January, 2, 3, 4, 0, 0, time.UTC))
	require.Equal(t, "ns-demo-nightly-20260102-030400", JobBackupName(run), "a scheduled run names its backup after its slot")
}

// jobNameLabels are the labels that name its Job on a pod the API server of Kubernetes 1.27 or later creates.
func jobNameLabels(job string) map[string]string {
	return map[string]string{LegacyJobNameLabel: job, batch.JobNameLabel: job}
}

func TestScheduledBackupName(t *testing.T) {
	due := time.Date(2026, time.January, 2, 3, 4, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		mutate func(*batch.Job)
		backup string
	}{
		"a run of the CronJob": {backup: "ns-demo-nightly-20260102-030400"},
		"a Job created by hand from the CronJob": {mutate: func(job *batch.Job) {
			job.Name = "nightly-main-backup-manual"
			job.Spec.Template.Labels = jobNameLabels(job.Name)
		}},
		"a slot with a sign": {mutate: func(job *batch.Job) {
			job.Name = "nightly-main-backup-+29433424"
			job.Spec.Template.Labels = jobNameLabels(job.Name)
		}},
		"a slot past the four-digit years": {mutate: func(job *batch.Job) {
			job.Name = "nightly-main-backup-9999999999"
			job.Spec.Template.Labels = jobNameLabels(job.Name)
		}},
		"a run on Kubernetes 1.25 or 1.26, whose pods carry the legacy Job name label alone": {
			mutate: func(job *batch.Job) {
				job.Spec.Template.Labels = map[string]string{LegacyJobNameLabel: job.Name}
			},
			backup: "ns-demo-nightly-20260102-030400",
		},
		// The Job script reads the legacy label alone, and names the backup of a pod without it after the time it runs
		"a run whose pods carry the Job name label of Kubernetes 1.27 alone": {
			mutate: func(job *batch.Job) {
				job.Spec.Template.Labels = map[string]string{batch.JobNameLabel: job.Name}
			},
		},
		"a Job of an API server without the Job name labels": {mutate: func(job *batch.Job) {
			job.Spec.Template.Labels = nil
		}},
		"a one-off backup": {mutate: func(job *batch.Job) {
			*job = *BuildBackupJob(testBackup("b1"), testInstallation(), testTopology())
		}},
	} {
		t.Run(name, func(t *testing.T) {
			job := scheduledRun(testSchedule("nightly"), due)
			if tc.mutate != nil {
				tc.mutate(job)
			}
			require.Equal(t, tc.backup, ScheduledBackupName(job))
		})
	}
}

// Why a Job failed outlives the Job's log: the script writes it to the termination message of
// the container, and a container that dies without writing one reports the tail of its log.
func TestJobsReportWhyTheyFailed(t *testing.T) {
	for name, spec := range map[string]core.PodSpec{
		"backup":  BuildBackupJob(testBackup("b1"), testInstallation(), testTopology()).Spec.Template.Spec,
		"verify":  BuildVerifyJob(testBackup("b1"), testInstallation(), testTopology()).Spec.Template.Spec,
		"restore": BuildRestoreJob(testRestore("r1"), testInstallation(), testTopology()).Spec.Template.Spec,
		"cronjob": BuildBackupCronJob(testSchedule("s1"), testInstallation(), testTopology(), false).Spec.JobTemplate.Spec.Template.Spec,
	} {
		t.Run(name, func(t *testing.T) {
			c := container(t, spec)
			require.Equal(t, core.TerminationMessageFallbackToLogsOnError, c.TerminationMessagePolicy)
			require.Equal(t, core.TerminationMessagePathDefault, c.TerminationMessagePath)
			require.Equal(t, c.TerminationMessagePath, env(c)[envTerminationMessagePath], "the script writes where the container reads")
		})
	}
}

func TestJobTerminationMessage(t *testing.T) {
	terminated := func(message string) core.ContainerState {
		return core.ContainerState{Terminated: &core.ContainerStateTerminated{ExitCode: 1, Message: message}}
	}
	for name, tc := range map[string]struct {
		status  core.ContainerStatus
		message string
	}{
		"terminated":          {status: core.ContainerStatus{Name: jobContainerName, State: terminated("backup failed\n")}, message: "backup failed"},
		"restarted":           {status: core.ContainerStatus{Name: jobContainerName, LastTerminationState: terminated("backup failed")}, message: "backup failed"},
		"running":             {status: core.ContainerStatus{Name: jobContainerName, State: core.ContainerState{Running: &core.ContainerStateRunning{}}}},
		"an injected sidecar": {status: core.ContainerStatus{Name: "istio-proxy", State: terminated("proxy failed")}},
		"terminated silently": {status: core.ContainerStatus{Name: jobContainerName, State: terminated("")}},
	} {
		t.Run(name, func(t *testing.T) {
			pod := &core.Pod{Status: core.PodStatus{ContainerStatuses: []core.ContainerStatus{tc.status}}}
			require.Equal(t, tc.message, JobTerminationMessage(pod))
		})
	}
}

// The Job script tells a backup that holds no data by the termination message its environment
// names, which only the Job container's own message tells, and only once it succeeded.
func TestJobBackupHoldsNoData(t *testing.T) {
	exited := func(code int32, message string) core.ContainerState {
		return core.ContainerState{Terminated: &core.ContainerStateTerminated{ExitCode: code, Message: message}}
	}
	declared := jobEnv(BuildBackupJob(testBackup("b1"), testInstallation(), testTopology()), envNoDataMessage)
	for name, tc := range map[string]struct {
		status core.ContainerStatus
		noData bool
	}{
		"succeeded without data": {status: core.ContainerStatus{Name: jobContainerName, State: exited(0, declared+"\n")}, noData: true},
		"restarted":              {status: core.ContainerStatus{Name: jobContainerName, LastTerminationState: exited(0, declared)}, noData: true},
		"succeeded with data":    {status: core.ContainerStatus{Name: jobContainerName, State: exited(0, "")}},
		"failed":                 {status: core.ContainerStatus{Name: jobContainerName, State: exited(1, declared)}},
		"another message":        {status: core.ContainerStatus{Name: jobContainerName, State: exited(0, "retention failed: "+declared)}},
		"running":                {status: core.ContainerStatus{Name: jobContainerName, State: core.ContainerState{Running: &core.ContainerStateRunning{}}}},
		"an injected sidecar":    {status: core.ContainerStatus{Name: "istio-proxy", State: exited(0, declared)}},
	} {
		t.Run(name, func(t *testing.T) {
			pod := &core.Pod{Status: core.PodStatus{ContainerStatuses: []core.ContainerStatus{tc.status}}}
			require.Equal(t, tc.noData, JobBackupHoldsNoData(pod))
		})
	}
}

// A message about a backup without data tells the table pattern the backup was made with, which a
// later edit of its resource does not change.
func TestJobTables(t *testing.T) {
	chb := testBackup("b1")
	chb.Spec.Tables = "db.*"
	job := BuildBackupJob(chb, testInstallation(), testTopology())
	chb.Spec.Tables = "other.*"
	require.Equal(t, "db.*", JobTables(job))
	require.Empty(t, JobTables(BuildBackupJob(testBackup("b2"), testInstallation(), testTopology())))
}

// A scheduled run tells the partitions it backed up, how many backups its retention keeps and the
// hosts it prunes on as its Job was built, which a later edit of the schedule does not change; the
// CronJob tells the hosts its runs prune on as it was last built.
func TestScheduledJobEnvironment(t *testing.T) {
	chbs := testSchedule("nightly")
	chbs.Spec.Partitions = []string{"202601", "202602"}
	chbs.Spec.Retention.KeepLast = ptr.To(int32(7))
	cronJob := BuildBackupCronJob(chbs, testInstallation(), testTopology(), false)
	run := scheduledRun(chbs, time.Date(2026, time.January, 2, 3, 0, 0, 0, time.UTC))
	chbs.Spec.Partitions = nil
	chbs.Spec.Retention.KeepLast = nil
	chbs.Spec.ReplicaSelection = api.ReplicaSelectionAllReplicas

	require.Equal(t, "202601,202602", JobPartitions(run))
	require.Equal(t, int32(7), JobKeepLast(run))
	require.Equal(t, []string{"chi-demo-main-0-0", "chi-demo-main-1-0"}, JobRetentionHosts(run))
	require.Equal(t, JobRetentionHosts(run), CronJobRetentionHosts(cronJob))

	oneOff := BuildBackupJob(testBackup("b1"), testInstallation(), testTopology())
	require.Empty(t, JobPartitions(oneOff))
	require.Zero(t, JobKeepLast(oneOff))
	require.Empty(t, JobRetentionHosts(oneOff))
	require.Empty(t, CronJobRetentionHosts(nil))
}

// The Job script exits with the code its environment names when it failed before it changed
// anything, which only the Job container's own exit code tells.
func TestJobFailedUnchanged(t *testing.T) {
	exited := func(code int32) core.ContainerState {
		return core.ContainerState{Terminated: &core.ContainerStateTerminated{ExitCode: code}}
	}
	job := BuildBackupJob(testBackup("b1"), testInstallation(), testTopology())
	declared, err := strconv.Atoi(jobEnv(job, envUnchangedExitCode))
	require.NoError(t, err)
	unchanged := int32(declared)
	for name, tc := range map[string]struct {
		status    core.ContainerStatus
		unchanged bool
	}{
		"failed unchanged":    {status: core.ContainerStatus{Name: jobContainerName, State: exited(unchanged)}, unchanged: true},
		"restarted":           {status: core.ContainerStatus{Name: jobContainerName, LastTerminationState: exited(unchanged)}, unchanged: true},
		"failed after change": {status: core.ContainerStatus{Name: jobContainerName, State: exited(1)}},
		"killed":              {status: core.ContainerStatus{Name: jobContainerName, State: exited(137)}},
		"running":             {status: core.ContainerStatus{Name: jobContainerName, State: core.ContainerState{Running: &core.ContainerStateRunning{}}}},
		"an injected sidecar": {status: core.ContainerStatus{Name: "istio-proxy", State: exited(unchanged)}},
	} {
		t.Run(name, func(t *testing.T) {
			pod := &core.Pod{Status: core.PodStatus{ContainerStatuses: []core.ContainerStatus{tc.status}}}
			require.Equal(t, tc.unchanged, JobFailedUnchanged(pod))
		})
	}
}

func TestSelectors(t *testing.T) {
	backupLabels := k8sLabels.Set(Labels(api.ClickHouseBackupCRDResourceKind, "b1", "demo", "main", testStorage))
	restoreLabels := k8sLabels.Set(Labels(api.ClickHouseRestoreCRDResourceKind, "r1", "demo", "main", testStorage))
	otherLabels := k8sLabels.Set(Labels(api.ClickHouseRestoreCRDResourceKind, "r2", "other", "main", testStorage))
	otherClusterLabels := k8sLabels.Set(Labels(api.ClickHouseRestoreCRDResourceKind, "r3", "demo", "second", testStorage))
	chiLabels := k8sLabels.Set{"clickhouse.altinity.com/app": "chop", "clickhouse.altinity.com/chi": "demo"}

	require.True(t, JobSelector().Matches(backupLabels))
	require.True(t, JobSelector().Matches(restoreLabels))
	require.False(t, JobSelector().Matches(chiLabels), "installation objects are not backup Jobs")

	require.True(t, CHIJobsSelector("demo", "").Matches(backupLabels))
	require.True(t, CHIJobsSelector("demo", "").Matches(restoreLabels))
	require.False(t, CHIJobsSelector("demo", "").Matches(otherLabels))
	require.False(t, CHIJobsSelector("demo", api.ClickHouseRestoreCRDResourceKind).Matches(backupLabels))

	require.True(t, CHIJobsSelector("demo", "").Matches(otherClusterLabels))

	require.True(t, ClusterJobsSelector("demo", "main", "").Matches(backupLabels))
	require.True(t, ClusterJobsSelector("demo", "main", "").Matches(restoreLabels))
	require.False(t, ClusterJobsSelector("demo", "main", "").Matches(otherClusterLabels), "an operation on another cluster does not conflict")
	require.False(t, ClusterJobsSelector("demo", "main", "").Matches(otherLabels))
	require.False(t, ClusterJobsSelector("demo", "main", api.ClickHouseRestoreCRDResourceKind).Matches(backupLabels))
	require.Equal(t, "second", ObjectCluster(&batch.Job{ObjectMeta: meta.ObjectMeta{Labels: otherClusterLabels}}))

	require.True(t, OwnedJobsSelector(api.ClickHouseBackupCRDResourceKind, "b1").Matches(backupLabels))
	require.False(t, OwnedJobsSelector(api.ClickHouseBackupCRDResourceKind, "b2").Matches(backupLabels))

	for key := range backupLabels {
		require.NotContains(t, chiLabels, key, "backup labels must not reuse the keys the CHI controller selects by")
	}
}

// A recovery restores one cluster of a new installation, its access entities included.
func TestRecoveryRestore(t *testing.T) {
	chi := testInstallation()
	restore := BuildRecoveryRestore(chi, ClusterRecovery{Cluster: "cold", BackupName: "nightly-20260101-000000"})

	require.Equal(t, "demo-cold-recovery", restore.Name)
	require.Equal(t, chi.Namespace, restore.Namespace)
	require.Equal(t, "demo", restore.Spec.ClickHouseInstallation)
	require.Equal(t, "cold", restore.Spec.Cluster)
	require.Equal(t, "nightly-20260101-000000", restore.Spec.BackupName)
	require.True(t, restore.Spec.RBAC.IsTrue())
	require.NoError(t, restore.Spec.Validate())
}

// A restore of a backup another installation made tells the Job where that installation keeps it: the values its
// {installation} and {namespace} macros take, set only when named, so a restore of the own backups runs as ever. The
// path check knows the namespace of the restored installation either way.
func TestRestoreJobKnowsTheSourceOfTheBackup(t *testing.T) {
	own := container(t, BuildRestoreJob(testRestore("r1"), testInstallation(), testTopology()).Spec.Template.Spec)
	require.NotContains(t, env(own), envSourceInstallation)
	require.NotContains(t, env(own), envSourceNamespace)
	require.Equal(t, testNamespace, env(own)[envPathNamespace])

	chr := testRestore("r1")
	chr.Spec.SourceInstallation = "prod-eu"
	chr.Spec.SourceNamespace = "team-a"
	other := env(container(t, BuildRestoreJob(chr, testInstallation(), testTopology()).Spec.Template.Spec))
	require.Equal(t, "prod-eu", other[envSourceInstallation])
	require.Equal(t, "team-a", other[envSourceNamespace])
}

// A recovery takes the installation that made its backup from the recovery it inherits, and hands it to the restore
// it creates.
func TestRecoveryRestoresFromItsSource(t *testing.T) {
	chi := newCHI("demo", &api.Cluster{Name: "hot", Backup: enabledBackup()}, &api.Cluster{Name: "cold", Backup: enabledBackup()})
	chi.Spec.Configuration.Clusters[1].Backup.Recovery = &api.BackupRecovery{SourceInstallation: types.NewString("cold-src")}
	chi.Spec.Backup = &api.ClusterBackup{Recovery: &api.BackupRecovery{
		BackupName:         types.NewString("nightly-20261004-020000"),
		SourceInstallation: types.NewString("prod"),
		SourceNamespace:    types.NewString("team-a"),
	}}
	_, stored := normalize(t, chi)

	recoveries := Recoveries(stored)
	require.Equal(t, []ClusterRecovery{
		{Cluster: "hot", BackupName: "nightly-20261004-020000", SourceInstallation: "prod", SourceNamespace: "team-a"},
		{Cluster: "cold", BackupName: "nightly-20261004-020000", SourceInstallation: "cold-src", SourceNamespace: "team-a"},
	}, recoveries)
	restore := BuildRecoveryRestore(stored, recoveries[1])
	require.Equal(t, "cold-src", restore.Spec.SourceInstallation)
	require.Equal(t, "team-a", restore.Spec.SourceNamespace)
	require.NoError(t, restore.Spec.Validate())
}

// Every backup-enabled cluster that inherits a non-empty backup name is recovered; a cluster that
// opts out with "", or has no sidecar to restore through, is not. A cluster that turns backup on without a
// storage is told apart, as its recovery waits for one.
func TestRecoveries(t *testing.T) {
	chi := newCHI("demo",
		&api.Cluster{Name: "hot", Backup: enabledBackup()},
		&api.Cluster{Name: "cold", Backup: &api.ClusterBackup{
			Enabled: types.NewStringBool(true), Recovery: &api.BackupRecovery{BackupName: types.NewString("cold-20261001-030000")},
		}},
		&api.Cluster{Name: "scratch", Backup: &api.ClusterBackup{
			Enabled: types.NewStringBool(true), Recovery: &api.BackupRecovery{BackupName: types.NewString("")},
		}},
		&api.Cluster{Name: "plain", Backup: &api.ClusterBackup{Enabled: types.NewStringBool(false)}},
		&api.Cluster{Name: "lacking", Backup: &api.ClusterBackup{
			Enabled: types.NewStringBool(true), Storage: types.NewString(""),
		}},
	)
	chi.Spec.Backup = &api.ClusterBackup{
		Storages: []api.BackupStorage{{Name: testStorage, SecretRef: &core.LocalObjectReference{Name: testStorageSecret}}},
		Recovery: &api.BackupRecovery{BackupName: types.NewString("nightly-20261004-020000")},
	}
	_, stored := normalize(t, chi)

	require.Equal(t, []ClusterRecovery{
		{Cluster: "hot", BackupName: "nightly-20261004-020000"},
		{Cluster: "cold", BackupName: "cold-20261001-030000"},
	}, Recoveries(stored))
	require.Equal(t, []ClusterRecovery{{Cluster: "lacking", BackupName: "nightly-20261004-020000"}}, RecoveriesWithoutStorage(stored),
		"a cluster whose backup a missing storage leaves off is not recovered, and its recovery is told apart")
	require.Empty(t, Recoveries(nil))
}

// The record of a recovery names the backup, or, when the clusters were recovered from different
// backups, each cluster with its backup; either reads back as the backups it names.
func TestRecoveredFrom(t *testing.T) {
	same := []ClusterRecovery{{Cluster: "hot", BackupName: "nightly"}, {Cluster: "cold", BackupName: "nightly"}}
	require.Equal(t, "nightly", RecoveredFrom(same))
	require.Equal(t, []string{"nightly"}, RecoveredBackupNames(RecoveredFrom(same)))

	different := []ClusterRecovery{{Cluster: "hot", BackupName: "nightly"}, {Cluster: "cold", BackupName: "weekly"}}
	require.Equal(t, "cold:weekly,hot:nightly", RecoveredFrom(different), "sorted by cluster")
	require.Equal(t, []string{"weekly", "nightly"}, RecoveredBackupNames(RecoveredFrom(different)))

	require.Equal(t, []string{"by-hand"}, RecoveredBackupNames("by-hand"))
	require.Empty(t, RecoveredBackupNames(""))
}

// A Job tells the hosts it drives as it was built, and a restore that left access entities of the
// backup unrestored tells so by the termination message its environment names.
func TestJobHostsAndRBACNotRestored(t *testing.T) {
	require.Equal(t, []string{"chi-demo-main-0-0", "chi-demo-main-1-0"}, JobHosts(BuildBackupJob(testBackup("b1"), testInstallation(), testTopology())))
	require.Equal(t, []string{"chi-demo-main-0-0", "chi-demo-main-1-0"}, JobHosts(BuildVerifyJob(testBackup("b1"), testInstallation(), testTopology())))
	require.Equal(t, []string{"chi-demo-main-0-0", "chi-demo-main-0-1", "chi-demo-main-1-0", "chi-demo-main-1-1"},
		JobHosts(BuildRestoreJob(testRestore("r1"), testInstallation(), testTopology())))

	declared := jobEnv(BuildRestoreJob(testRestore("r1"), testInstallation(), testTopology()), envRBACNotRestoredMessage)
	exited := func(code int32, message string) *core.Pod {
		return &core.Pod{Status: core.PodStatus{ContainerStatuses: []core.ContainerStatus{{
			Name: jobContainerName, State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ExitCode: code, Message: message}},
		}}}}
	}
	require.True(t, JobRBACNotRestored(exited(0, declared+"\n")))
	require.False(t, JobRBACNotRestored(exited(1, declared)))
	require.False(t, JobRBACNotRestored(exited(0, "")))
}

// pathCheckInstallation returns a stored normalized installation with clusters whose sidecars take
// their storage settings from every kind of source, one whose sidecar is built on a container that
// sets none, and one without backup.
func pathCheckInstallation(t *testing.T) *api.ClickHouseInstallation {
	t.Helper()
	hot := layoutCluster("hot", 2, 2, &api.ClusterBackup{
		Enabled:  types.NewStringBool(true),
		Storages: []api.BackupStorage{{Name: "main", SecretRef: &core.LocalObjectReference{Name: "s3"}}},
	})
	cold := layoutCluster("cold", 1, 1, &api.ClusterBackup{
		Enabled:  types.NewStringBool(true),
		Storages: []api.BackupStorage{{Name: "cold", SecretRef: &core.LocalObjectReference{Name: "s3-cold"}}},
	})
	plain := layoutCluster("plain", 1, 1, nil)
	bare := layoutCluster("bare", 1, 1, &api.ClusterBackup{Enabled: types.NewStringBool(true)})
	chi := newCHI("demo", hot, plain, cold, bare)
	chi.Spec.Templates = &api.Templates{PodTemplates: []api.PodTemplate{{
		Name: "own-sidecar",
		Spec: core.PodSpec{Containers: []core.Container{
			{Name: config.ClickHouseContainerName, Image: "clickhouse/clickhouse-server"},
			{Name: config.ClickHouseBackupContainerName, Image: "registry/backup", Command: []string{"clickhouse-backup", "server"}},
		}},
	}, {
		Name: "hot-env",
		Spec: core.PodSpec{Containers: []core.Container{
			{Name: config.ClickHouseContainerName, Image: "clickhouse/clickhouse-server"},
			{Name: config.ClickHouseBackupContainerName, Env: []core.EnvVar{
				{Name: "S3_PATH", Value: "ignored"},
				{Name: "S3_PATH", Value: "backup/{cluster}/{shard}"},
				{Name: "S3_BUCKET", ValueFrom: &core.EnvVarSource{SecretKeyRef: &core.SecretKeySelector{
					LocalObjectReference: core.LocalObjectReference{Name: "bucket"}, Key: "name",
				}}},
			}},
		}},
	}}}
	sidecarTemplate(chi, "bare-sidecar", core.Container{})
	useTemplate(chi, hot, "hot-env")
	useTemplate(chi, cold, "own-sidecar")
	useTemplate(chi, bare, "bare-sidecar")
	_, stored := normalize(t, chi)
	return stored
}

// The Jobs check the paths of a sidecar that sets none with the defaults the operator configuration gives it.
func TestJobsCarryTheConfiguredDefaultPaths(t *testing.T) {
	defaults := chop.Config().Backup
	t.Cleanup(func() { chop.Config().Backup = defaults })
	chop.Config().Backup = api.OperatorConfigBackup{
		StoragePath:           "eu1/backup/{namespace}/{installation}/{cluster}/{shard}",
		StorageObjectDiskPath: "eu1/object_disks/{namespace}/{installation}/{cluster}/{shard}",
	}
	stored := pathCheckInstallation(t)
	topology, err := NewTopology(stored, "")
	require.NoError(t, err)
	cold, err := topology.ForCluster("cold")
	require.NoError(t, err)
	values := env(container(t, BuildRestoreJob(testRestore("r1"), stored, cold).Spec.Template.Spec))
	require.Equal(t, chop.Config().Backup.StoragePath, values["DEFAULT_PATH"])
	require.Equal(t, chop.Config().Backup.StorageObjectDiskPath, values["DEFAULT_OBJECT_DISK_PATH"])
	require.Equal(t, testNamespace, values[envPathNamespace], "the value of {namespace} on the hosts")
}

// Before it changes anything a Job checks that the storage paths of the hosts it drives are their own,
// across every backup-enabled cluster of the installation, which it therefore knows all of: by index,
// with their shards and every setting that tells where their sidecars keep their backups, taken from
// where the sidecars take it: the configuration file Secret of the cluster, which kubelet resolves for the Job,
// optionally, so that a check never keeps the Job from starting, under the env of the pod template's container.
func TestJobsCarryWhatThePathCheckNeeds(t *testing.T) {
	stored := pathCheckInstallation(t)
	topology, err := NewTopology(stored, "")
	require.NoError(t, err)
	require.Equal(t, []string{"hot", "cold", "bare"}, topology.ClusterNames())
	cold, err := topology.ForCluster("cold")
	require.NoError(t, err)

	chbs := testSchedule("s1")
	chbs.Spec.ReplicaSelection = api.ReplicaSelectionAllReplicas
	for name, tc := range map[string]struct {
		spec      core.PodSpec
		self      string
		selection string
	}{
		"backup":  {spec: BuildBackupJob(testBackup("b1"), stored, cold).Spec.Template.Spec, self: "1", selection: string(api.ReplicaSelectionFirstPerShard)},
		"cronjob": {spec: BuildBackupCronJob(chbs, stored, cold, false).Spec.JobTemplate.Spec.Template.Spec, self: "1", selection: string(api.ReplicaSelectionAllReplicas)},
		"restore": {spec: BuildRestoreJob(testRestore("r1"), stored, cold).Spec.Template.Spec, self: "1"},
	} {
		t.Run(name, func(t *testing.T) {
			c := container(t, tc.spec)
			values := env(c)
			require.Equal(t, "cold", values[envCluster])
			require.Equal(t, "0 1 2", values[envPathClusters], "every backup-enabled cluster, the one without backup left out")
			require.Equal(t, tc.self, values[envPathSelf])
			require.Equal(t, "demo", values[envPathInstallation])
			if tc.selection == "" {
				require.NotContains(t, values, envPathReplicaSelection)
			} else {
				require.Equal(t, tc.selection, values[envPathReplicaSelection])
			}

			require.Equal(t, chop.Config().Backup.StoragePath, values["DEFAULT_PATH"], "the paths the operator's sidecar defaults to")
			require.Equal(t, chop.Config().Backup.StorageObjectDiskPath, values["DEFAULT_OBJECT_DISK_PATH"])

			require.Equal(t, "hot", values["PATH_0_CLUSTER"])
			require.Equal(t, "0 1", values["PATH_0_SHARDS"], "the {shard} macro values of the shards")
			require.NotContains(t, values, "PATH_0_DEFAULT_PATH", "every cluster takes the defaults of its configuration file")
			require.NotContains(t, values, "PATH_0_DEFAULT_OBJECT_DISK_PATH")
			require.Equal(t, "backup/{cluster}/{shard}", values["PATH_0_S3_PATH"], "a value of the pod template's container, the last one as for the sidecar")
			bucket := envVarNamed(t, c, "PATH_0_S3_BUCKET").ValueFrom.SecretKeyRef
			require.Equal(t, "bucket", bucket.Name, "a source of the pod template's container")
			require.Equal(t, "name", bucket.Key)
			require.True(t, ptr.Deref(bucket.Optional, false))
			for _, key := range PathCheckKeys {
				name := "PATH_0_" + key
				if (key == "S3_PATH") || (key == "S3_BUCKET") {
					continue
				}
				fromSecret := envVarNamed(t, c, name).ValueFrom.SecretKeyRef
				require.Equal(t, "demo-hot-backup-config", fromSecret.Name, name)
				require.Equal(t, key, fromSecret.Key, name)
				require.True(t, ptr.Deref(fromSecret.Optional, false), name)
			}

			require.Equal(t, "cold", values["PATH_1_CLUSTER"])
			require.Equal(t, "0", values["PATH_1_SHARDS"])
			require.NotContains(t, values, "PATH_1_DEFAULT_PATH", "a sidecar running a command of its own reads the configuration file too")
			require.Equal(t, "demo-cold-backup-config", envVarNamed(t, c, "PATH_1_REMOTE_STORAGE").ValueFrom.SecretKeyRef.Name)

			require.Equal(t, "bare", values["PATH_2_CLUSTER"])
			require.Equal(t, "demo-bare-backup-config", envVarNamed(t, c, "PATH_2_S3_PATH").ValueFrom.SecretKeyRef.Name,
				"a cluster without a storage Secret has the configuration file all the same")
			require.NotContains(t, values, "PATH_3_CLUSTER")
			for _, e := range c.Env {
				require.NotContains(t, e.Name, "plain")
				require.NotEqual(t, "plain", e.Value)
			}
		})
	}
}

// A Job drives the hosts of one cluster only, with the image and the pod settings of that cluster.
func TestJobsTargetOneCluster(t *testing.T) {
	stored := pathCheckInstallation(t)
	topology, err := NewTopology(stored, "")
	require.NoError(t, err)
	hot, err := topology.ForCluster("hot")
	require.NoError(t, err)
	cold, err := topology.ForCluster("cold")
	require.NoError(t, err)

	hotJob := BuildBackupJob(testBackup("b1"), stored, hot)
	coldJob := BuildBackupJob(testBackup("b1"), stored, cold)
	require.Equal(t, "b1-hot-backup", hotJob.Name)
	require.Equal(t, "b1-cold-backup", coldJob.Name)
	require.Equal(t, []string{"chi-demo-hot-0-0", "chi-demo-hot-1-0"}, JobHosts(hotJob))
	require.Equal(t, []string{"chi-demo-cold-0-0"}, JobHosts(coldJob))
	require.Equal(t, JobBackupName(hotJob), JobBackupName(coldJob), "every cluster makes the backup under one name")
	for cluster, job := range map[string]*batch.Job{"hot": hotJob, "cold": coldJob} {
		require.Equal(t, cluster, job.Labels[LabelBackupCluster])
		require.Equal(t, cluster, job.Spec.Template.Labels[LabelBackupCluster], "the pods of a Job tell its cluster too")
	}
	cronJob := BuildBackupCronJob(testSchedule("s1"), stored, cold, false)
	require.Equal(t, "s1-cold-backup", cronJob.Name)
	require.Equal(t, "cold", cronJob.Labels[LabelBackupCluster])
	require.Equal(t, "cold", cronJob.Spec.JobTemplate.Labels[LabelBackupCluster])
	require.Equal(t, "cold", cronJob.Spec.JobTemplate.Spec.Template.Labels[LabelBackupCluster])
}

// Every Job and CronJob carries the storage of the cluster it targets, which the status of its resource records:
// clusters of one installation may keep their backups in storages of their own.
func TestJobsLabelTheStorageOfTheirCluster(t *testing.T) {
	stored := pathCheckInstallation(t)
	for cluster, storage := range map[string]string{"hot": "main", "cold": "cold"} {
		topology, err := NewTopology(stored, cluster)
		require.NoError(t, err)
		for kind, obj := range map[string]meta.Object{
			"backup":  BuildBackupJob(testBackup("b1"), stored, topology),
			"verify":  BuildVerifyJob(testBackup("b1"), stored, topology),
			"restore": BuildRestoreJob(testRestore("r1"), stored, topology),
			"cronjob": BuildBackupCronJob(testSchedule("s1"), stored, topology, false),
		} {
			require.Equal(t, storage, ObjectStorage(obj), "%s of cluster %s", kind, cluster)
		}
		cronJob := BuildBackupCronJob(testSchedule("s1"), stored, topology, false)
		require.Equal(t, storage, ObjectStorage(&cronJob.Spec.JobTemplate), "the Jobs a CronJob of cluster %s starts", cluster)
	}
}
