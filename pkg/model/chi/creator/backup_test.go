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

// External test package (creator_test) so it can import managers to build a real creator;
// managers imports creator, so an in-package test would form an import cycle.
package creator_test

import (
	"path"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/backupconfig"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	chiCreator "github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/macro"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	chiNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

func init() { chop.New(nil, nil, "") }

const (
	crName           = "chi-backup"
	crNamespace      = "ns"
	backupSecretName = crName + "-auto-backup-secret"
	backedUpCluster  = "backed-up"
	plainCluster     = "plain"
	storageSecret    = "s3-credentials"
	podTemplateName  = "pt"
	serverImage      = "clickhouse/clickhouse-server:25.3"
	dataVCT          = "data"
	diskVCT          = "disk2"
	diskMountPath    = "/var/lib/clickhouse/disks/disk2"
)

// buildCreator mirrors worker.buildCreator (pkg/controller/chi/worker.go) so the objects under test
// are the ones the controller ships.
func buildCreator(cr *chi.ClickHouseInstallation) *commonCreator.Creator {
	return commonCreator.NewCreator(
		cr,
		managers.NewConfigFilesGenerator(managers.FilesGeneratorTypeClickHouse, cr, &config.GeneratorOptions{
			Users:    cr.GetSpecT().GetConfiguration().GetUsers(),
			Profiles: cr.GetSpecT().GetConfiguration().GetProfiles(),
			Quotas:   cr.GetSpecT().GetConfiguration().GetQuotas(),
			Settings: cr.GetSpecT().GetConfiguration().GetSettings(),
			Files:    cr.GetSpecT().GetConfiguration().GetFiles(),
		}),
		managers.NewContainerManager(managers.ContainerManagerTypeClickHouse),
		managers.NewTagManager(managers.TagManagerTypeClickHouse, cr),
		managers.NewProbeManager(managers.ProbeManagerTypeClickHouse),
		managers.NewServiceManager(managers.ServiceManagerTypeClickHouse),
		managers.NewVolumeManager(managers.VolumeManagerTypeClickHouse),
		managers.NewConfigMapManager(managers.ConfigMapManagerTypeClickHouse),
		managers.NewNameManager(managers.NameManagerTypeClickHouse),
		managers.NewOwnerReferencesManager(managers.OwnerReferencesManagerTypeClickHouse),
		namer.New(),
		macro.New(),
		labeler.New(cr),
	)
}

// newCR builds an installation whose ClickHouse container mounts a second disk on top of the data volume,
// as the sidecar has to see every disk of the server.
func newCR(clusters ...*chi.Cluster) *chi.ClickHouseInstallation {
	cr := &chi.ClickHouseInstallation{}
	cr.Name = crName
	cr.Namespace = crNamespace
	cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{
		PodTemplate:             podTemplateName,
		DataVolumeClaimTemplate: dataVCT,
	}}
	cr.Spec.Templates = &chi.Templates{
		PodTemplates: []chi.PodTemplate{{
			Name: podTemplateName,
			Spec: core.PodSpec{Containers: []core.Container{{
				Name:         config.ClickHouseContainerName,
				Image:        serverImage,
				VolumeMounts: []core.VolumeMount{{Name: diskVCT, MountPath: diskMountPath}},
			}}},
		}},
		VolumeClaimTemplates: []chi.VolumeClaimTemplate{{Name: dataVCT}, {Name: diskVCT}},
	}
	cr.Spec.Configuration = &chi.Configuration{Clusters: clusters}
	return cr
}

func newCluster(name string, backup *chi.ClusterBackup) *chi.Cluster {
	return &chi.Cluster{
		Name:   name,
		Layout: &chi.ChiClusterLayout{ShardsCount: 1, ReplicasCount: 1},
		Backup: backup,
	}
}

// sidecarOn gives the cluster a pod template of its own: the shared one with the clickhouse-backup container given,
// which the cluster's backup builds the sidecar on
func sidecarOn(cr *chi.ClickHouseInstallation, cluster *chi.Cluster, sidecar core.Container) {
	template := *cr.Spec.Templates.PodTemplates[0].DeepCopy()
	template.Name = cluster.Name
	sidecar.Name = config.ClickHouseBackupContainerName
	template.Spec.Containers = append(template.Spec.Containers, sidecar)
	cr.Spec.Templates.PodTemplates = append(cr.Spec.Templates.PodTemplates, template)
	cluster.Templates = &chi.TemplatesList{PodTemplate: template.Name}
	if cluster.Backup == nil {
		cluster.Backup = &chi.ClusterBackup{}
	}
	cluster.Backup.PodTemplate = types.NewString(template.Name)
}

func enabled() *chi.ClusterBackup {
	return &chi.ClusterBackup{
		Enabled:  types.NewStringBool(true),
		Storages: []chi.BackupStorage{{Name: "main", SecretRef: &core.LocalObjectReference{Name: storageSecret}}},
	}
}

// enabledOn enables backup with the sidecar built on the clickhouse-backup container of the pod template named
func enabledOn(podTemplate string) *chi.ClusterBackup {
	backup := enabled()
	backup.PodTemplate = types.NewString(podTemplate)
	return backup
}

// backupSecretGetter serves the backup Secret as the operator creates it ahead of normalization, so the
// backup user is in place whenever backup is enabled
func backupSecretGetter(namespace, name string) (*core.Secret, error) {
	if name != backupSecretName {
		return nil, apiErrors.NewNotFound(core.Resource("secrets"), name)
	}
	return &core.Secret{Data: map[string][]byte{
		config.ClickHouseBackupSecretKeyClickHousePassword: []byte("GENERATEDBYTHEOPERATOR2345"),
	}}, nil
}

func normalize(t *testing.T, cr *chi.ClickHouseInstallation) *chi.ClickHouseInstallation {
	t.Helper()
	got, err := chiNormalizer.New(backupSecretGetter).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
	require.NoError(t, err)
	require.NotEqual(t, chi.StatusAborted, got.EnsureStatus().GetStatus(), got.EnsureStatus().GetErrors())
	return got
}

func findHost(t *testing.T, cr *chi.ClickHouseInstallation, cluster string) *chi.Host {
	t.Helper()
	var found *chi.Host
	cr.WalkHosts(func(host *chi.Host) error {
		if (found == nil) && (host.Runtime.Address.ClusterName == cluster) {
			found = host
		}
		return nil
	})
	require.NotNil(t, found, "no host in cluster %q", cluster)
	return found
}

// createStatefulSet creates the StatefulSet of the first host of the cluster
func createStatefulSet(t *testing.T, cr *chi.ClickHouseInstallation, cluster string) (*apps.StatefulSet, *chi.Host) {
	t.Helper()
	host := findHost(t, cr, cluster)
	sts := buildCreator(cr).CreateStatefulSet(host, false)
	require.NotNil(t, sts)
	return sts, host
}

func requireBackupContainer(t *testing.T, sts *apps.StatefulSet) *core.Container {
	t.Helper()
	container, ok := k8s.StatefulSetContainerGet(sts, config.ClickHouseBackupContainerName)
	require.True(t, ok, "StatefulSet %s has no backup sidecar", sts.Name)
	return container
}

func requireNoBackupContainer(t *testing.T, sts *apps.StatefulSet) {
	t.Helper()
	_, ok := k8s.StatefulSetContainerGet(sts, config.ClickHouseBackupContainerName)
	require.False(t, ok, "StatefulSet %s of a cluster without backup got the sidecar", sts.Name)
}

// envVar finds the single env var of the name - a second one would leave the effective value to chance
func envVar(t *testing.T, container *core.Container, name string) (core.EnvVar, bool) {
	t.Helper()
	var found []core.EnvVar
	for _, e := range container.Env {
		if e.Name == name {
			found = append(found, e)
		}
	}
	require.LessOrEqual(t, len(found), 1, "env var %s set %d times", name, len(found))
	if len(found) == 0 {
		return core.EnvVar{}, false
	}
	return found[0], true
}

func envValue(t *testing.T, container *core.Container, name string) string {
	t.Helper()
	e, ok := envVar(t, container, name)
	require.True(t, ok, "env var %s missing", name)
	return e.Value
}

// receivedCommand returns the command of a container as kubelet hands it to the container, see receivedArgs.
func receivedCommand(container *core.Container) []string {
	return receivedArgs(container, container.Command)
}

// receivedArgs returns args, a command of the container or of its exec probe, as kubelet runs it: with its
// $(VAR_NAME) references to the container's literal env vars expanded and "$$" reduced to "$".
func receivedArgs(container *core.Container, args []string) []string {
	lookup := func(name string) (string, bool) {
		for _, envVar := range container.Env {
			if (envVar.Name == name) && (envVar.ValueFrom == nil) {
				return envVar.Value, true
			}
		}
		return "", false
	}
	var received []string
	for _, arg := range args {
		received = append(received, k8s.ExpandVariableReferences(arg, lookup))
	}
	return received
}

// sidecarCommand is the command the operator gives the sidecar: clickhouse-backup server, started by its script,
// which gets the configuration file it waits for ahead of it
func sidecarCommand() []string {
	return []string{
		"bash", "-c", chiCreator.BackupSidecarScript, config.ClickHouseBackupContainerName,
		path.Join(config.DirPathClickHouseBackupConfig, backupconfig.FileName),
		config.ClickHouseBackupCommand, config.ClickHouseBackupCommandServer,
	}
}

// configSecretName is the name of the configuration file Secret of the cluster
func configSecretName(cluster string) string {
	return crName + "-" + cluster + "-backup-config"
}

// requireConfigVolume requires the pod to mount the configuration file Secret of the cluster into the sidecar alone,
// read-only at the directory clickhouse-backup reads its configuration file from, optional as given
func requireConfigVolume(t *testing.T, sts *apps.StatefulSet, cluster string, optional bool) {
	t.Helper()
	var volumes []core.Volume
	for _, volume := range sts.Spec.Template.Spec.Volumes {
		if volume.Name == config.ClickHouseBackupConfigVolumeName {
			volumes = append(volumes, volume)
		}
	}
	require.Equal(t, []core.Volume{{
		Name: config.ClickHouseBackupConfigVolumeName,
		VolumeSource: core.VolumeSource{Secret: &core.SecretVolumeSource{
			SecretName: configSecretName(cluster),
			Items:      []core.KeyToPath{{Key: backupconfig.FileName, Path: backupconfig.FileName}},
			Optional:   ptr.To(optional),
		}},
	}}, volumes)
	for _, container := range sts.Spec.Template.Spec.Containers {
		var mounts []core.VolumeMount
		for _, mount := range container.VolumeMounts {
			sidecarAt := (container.Name == config.ClickHouseBackupContainerName) && strings.HasPrefix(mount.MountPath, config.DirPathClickHouseBackupConfig)
			if (mount.Name == config.ClickHouseBackupConfigVolumeName) || sidecarAt {
				mounts = append(mounts, mount)
			}
		}
		if container.Name != config.ClickHouseBackupContainerName {
			require.Empty(t, mounts, "container %s mounts the configuration file", container.Name)
			continue
		}
		require.Equal(t, []core.VolumeMount{{
			Name:      config.ClickHouseBackupConfigVolumeName,
			MountPath: config.DirPathClickHouseBackupConfig,
			ReadOnly:  true,
		}}, mounts)
	}
}

func requireMountedAt(t *testing.T, container *core.Container, path string) {
	t.Helper()
	for _, m := range container.VolumeMounts {
		if m.MountPath == path {
			return
		}
	}
	require.Failf(t, "mount missing", "container %s mounts nothing at %s", container.Name, path)
}

// The sidecar is added to hosts of backup-enabled clusters only, wired to the host's ClickHouse, the configuration
// file Secret of its cluster, and every volume the ClickHouse container mounts. The credentials and the storage
// settings are in the configuration file, which clickhouse-backup reads anew for every operation, never in env vars,
// which would keep a change from the sidecar until it restarts.
func TestBackupSidecar(t *testing.T) {
	resources := &core.ResourceRequirements{
		Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("1")},
	}
	cr := newCR(newCluster(backedUpCluster, enabled()), newCluster(plainCluster, nil))
	cr.Spec.Backup = &chi.ClusterBackup{
		Storages: []chi.BackupStorage{{Name: "main", SecretRef: &core.LocalObjectReference{Name: storageSecret}}},
	}
	sidecarOn(cr, cr.Spec.Configuration.Clusters[0], core.Container{Env: []core.EnvVar{{Name: "LOG_LEVEL", Value: "debug"}}, Resources: *resources})
	cr = normalize(t, cr)

	sts, host := createStatefulSet(t, cr, backedUpCluster)
	sidecar := requireBackupContainer(t, sts)

	require.Equal(t, config.DefaultClickHouseBackupDockerImage, sidecar.Image)
	require.Equal(t, sidecarCommand(), receivedCommand(sidecar))

	require.Equal(t, "0.0.0.0:"+strconv.Itoa(config.ClickHouseBackupAPIPortNumber), envValue(t, sidecar, config.ClickHouseBackupEnvAPIListen))
	require.Equal(t, config.ClickHouseBackupClickHouseHost, envValue(t, sidecar, config.ClickHouseBackupEnvClickHouseHost))
	require.Equal(t, strconv.Itoa(int(host.TCPPort.Value())), envValue(t, sidecar, config.ClickHouseBackupEnvClickHousePort))
	require.Equal(t, config.ClickHouseBackupBackupsToKeepLocalDeleteAfterUpload, envValue(t, sidecar, config.ClickHouseBackupEnvBackupsToKeepLocal),
		"local copies of uploaded backups must not pile up on the data disk")
	require.Equal(t, strconv.FormatBool(true), envValue(t, sidecar, config.ClickHouseBackupEnvAllowEmptyBackups),
		"a backup of a cluster without tables must not fail")
	require.Equal(t, "debug", envValue(t, sidecar, "LOG_LEVEL"))
	_, secure := envVar(t, sidecar, config.ClickHouseBackupEnvClickHouseSecure)
	require.False(t, secure, "a host serving plain TCP needs no TLS")

	for _, key := range []string{
		config.ClickHouseBackupSecretKeyAPIUsername,
		config.ClickHouseBackupSecretKeyAPIPassword,
		config.ClickHouseBackupSecretKeyClickHouseUsername,
		config.ClickHouseBackupSecretKeyClickHousePassword,
		config.ClickHouseBackupEnvConfig,
	} {
		_, ok := envVar(t, sidecar, key)
		require.False(t, ok, "env var %s would override the configuration file", key)
	}
	require.Empty(t, sidecar.EnvFrom, "the storage settings are in the configuration file")
	require.Nil(t, sidecar.LivenessProbe, "nothing restarts the sidecar to apply a changed Secret")
	// Optional: the sidecar's own command waits for the file, rather than keep the pod from getting ready
	requireConfigVolume(t, sts, backedUpCluster, true)
	require.Contains(t, sidecar.Ports, core.ContainerPort{
		Name:          config.ClickHouseBackupAPIPortName,
		ContainerPort: config.ClickHouseBackupAPIPortNumber,
	})
	require.Equal(t, *resources, sidecar.Resources)

	clickHouse, ok := k8s.StatefulSetContainerGet(sts, config.ClickHouseContainerName)
	require.True(t, ok)
	require.NotEmpty(t, clickHouse.VolumeMounts)
	for _, m := range clickHouse.VolumeMounts {
		require.Contains(t, sidecar.VolumeMounts, m, "the sidecar misses ClickHouse mount %s at %s", m.Name, m.MountPath)
	}
	requireMountedAt(t, sidecar, config.DirPathDataStorage)
	requireMountedAt(t, sidecar, diskMountPath)

	plain, _ := createStatefulSet(t, cr, plainCluster)
	requireNoBackupContainer(t, plain)
}

// The sidecar gets every mount of the ClickHouse container as it is, also of a volume mounted more than once -
// at different paths, of different subPaths. A path the sidecar's base container mounts something at already
// keeps that mount: a second mount at one path would make the pod invalid.
func TestBackupSidecarVolumeMountsCopiedExactly(t *testing.T) {
	const (
		sharedVolume  = "shared"
		userMountPath = "/scratch"
	)
	clickHouseMounts := []core.VolumeMount{
		{Name: sharedVolume, MountPath: "/etc/clickhouse-server/extra.d", SubPath: "config", ReadOnly: true},
		{Name: sharedVolume, MountPath: "/var/lib/clickhouse/user_files", SubPath: "files"},
		{Name: sharedVolume, MountPath: userMountPath, SubPath: "scratch"},
	}
	ownMount := core.VolumeMount{Name: "own", MountPath: userMountPath}
	cr := newCR(newCluster(backedUpCluster, enabledOn(podTemplateName)))
	podSpec := &cr.Spec.Templates.PodTemplates[0].Spec
	podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, clickHouseMounts...)
	podSpec.Containers = append(podSpec.Containers, core.Container{Name: config.ClickHouseBackupContainerName, VolumeMounts: []core.VolumeMount{ownMount}})
	podSpec.Volumes = []core.Volume{
		{Name: sharedVolume, VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}},
		{Name: "own", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}},
	}

	sts, _ := createStatefulSet(t, normalize(t, cr), backedUpCluster)
	sidecar := requireBackupContainer(t, sts)
	clickHouse, _ := k8s.StatefulSetContainerGet(sts, config.ClickHouseContainerName)
	for _, mount := range clickHouse.VolumeMounts {
		if mount.MountPath == userMountPath {
			continue
		}
		require.Contains(t, sidecar.VolumeMounts, mount, "the sidecar misses ClickHouse mount %s at %s", mount.Name, mount.MountPath)
	}
	paths := map[string]core.VolumeMount{}
	for _, mount := range sidecar.VolumeMounts {
		require.NotContains(t, paths, mount.MountPath, "two mounts at %s", mount.MountPath)
		paths[mount.MountPath] = mount
	}
	require.Equal(t, ownMount, paths[userMountPath], "the base container's own mount is kept")
}

// The sidecar works on the server's files directly, so it must see ClickHouse's data path. Without a volume
// there the data would live in the ClickHouse container's own filesystem, out of the sidecar's reach, so both
// containers get a shared volume at the data path - a mount at a path within it does not count, one above does.
func TestBackupSidecarSharesDataPath(t *testing.T) {
	withoutDataVolume := func(clusters ...*chi.Cluster) *chi.ClickHouseInstallation {
		cr := newCR(clusters...)
		cr.Spec.Defaults.Templates.DataVolumeClaimTemplate = ""
		return cr
	}
	requireSharedDataVolume := func(t *testing.T, sts *apps.StatefulSet) {
		t.Helper()
		var volume *core.Volume
		for i := range sts.Spec.Template.Spec.Volumes {
			if sts.Spec.Template.Spec.Volumes[i].Name == config.ClickHouseBackupSharedDataVolumeName {
				volume = &sts.Spec.Template.Spec.Volumes[i]
			}
		}
		require.NotNil(t, volume, "no shared data volume in the pod")
		require.NotNil(t, volume.EmptyDir)
		shared := core.VolumeMount{Name: config.ClickHouseBackupSharedDataVolumeName, MountPath: config.DirPathDataStorage}
		clickHouse, ok := k8s.StatefulSetContainerGet(sts, config.ClickHouseContainerName)
		require.True(t, ok)
		require.Contains(t, clickHouse.VolumeMounts, shared)
		require.Contains(t, requireBackupContainer(t, sts).VolumeMounts, shared)
	}
	requireNoSharedDataVolume := func(t *testing.T, sts *apps.StatefulSet) {
		t.Helper()
		require.False(t, k8s.StatefulSetHasVolumeByName(sts, config.ClickHouseBackupSharedDataVolumeName))
		k8s.StatefulSetWalkVolumeMounts(sts, func(mount *core.VolumeMount) {
			require.NotEqual(t, config.ClickHouseBackupSharedDataVolumeName, mount.Name)
		})
	}

	t.Run("no data volume", func(t *testing.T) {
		cr := normalize(t, withoutDataVolume(newCluster(backedUpCluster, enabled()), newCluster(plainCluster, nil)))
		sts, _ := createStatefulSet(t, cr, backedUpCluster)
		requireSharedDataVolume(t, sts)
		requireMountedAt(t, requireBackupContainer(t, sts), diskMountPath)

		plain, _ := createStatefulSet(t, cr, plainCluster)
		requireNoSharedDataVolume(t, plain)
	})
	t.Run("data volume claim template", func(t *testing.T) {
		cr := normalize(t, newCR(newCluster(backedUpCluster, enabled())))
		sts, _ := createStatefulSet(t, cr, backedUpCluster)
		requireNoSharedDataVolume(t, sts)
	})
	t.Run("volume mounted above the data path", func(t *testing.T) {
		const parentVolume = "var-lib"
		cr := withoutDataVolume(newCluster(backedUpCluster, enabled()))
		podSpec := &cr.Spec.Templates.PodTemplates[0].Spec
		podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, core.VolumeMount{Name: parentVolume, MountPath: "/var/lib/"})
		podSpec.Volumes = []core.Volume{{Name: parentVolume, VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}}}
		sts, _ := createStatefulSet(t, normalize(t, cr), backedUpCluster)
		requireNoSharedDataVolume(t, sts)
		requireMountedAt(t, requireBackupContainer(t, sts), "/var/lib/")
	})
}

// A sidecar without a securityContext would get the pod rejected where ClickHouse itself is admitted - by Pod
// Security Admission "restricted" or a policy engine - so it runs as the ClickHouse container does, which also
// gives it the same access to the server's files. A securityContext the sidecar's base container has is kept.
func TestBackupSidecarSecurityContext(t *testing.T) {
	clickHouseSecurityContext := &core.SecurityContext{
		RunAsUser:                ptr.To(int64(101)),
		RunAsGroup:               ptr.To(int64(101)),
		RunAsNonRoot:             ptr.To(true),
		AllowPrivilegeEscalation: ptr.To(false),
		Capabilities:             &core.Capabilities{Drop: []core.Capability{"ALL"}},
		SeccompProfile:           &core.SeccompProfile{Type: core.SeccompProfileTypeRuntimeDefault},
	}
	newSecuredCR := func(sidecar ...core.Container) *chi.ClickHouseInstallation {
		backup := enabled()
		if len(sidecar) > 0 {
			backup = enabledOn(podTemplateName)
		}
		cr := newCR(newCluster(backedUpCluster, backup))
		podSpec := &cr.Spec.Templates.PodTemplates[0].Spec
		podSpec.Containers[0].SecurityContext = clickHouseSecurityContext.DeepCopy()
		podSpec.Containers = append(podSpec.Containers, sidecar...)
		return normalize(t, cr)
	}

	t.Run("copied from the ClickHouse container", func(t *testing.T) {
		sts, _ := createStatefulSet(t, newSecuredCR(), backedUpCluster)
		sidecar := requireBackupContainer(t, sts)
		require.Equal(t, clickHouseSecurityContext, sidecar.SecurityContext)
		clickHouse, _ := k8s.StatefulSetContainerGet(sts, config.ClickHouseContainerName)
		require.NotSame(t, clickHouse.SecurityContext, sidecar.SecurityContext, "the two containers must not share one securityContext")
	})
	t.Run("the base container's own is kept", func(t *testing.T) {
		own := &core.SecurityContext{RunAsUser: ptr.To(int64(0))}
		sts, _ := createStatefulSet(t, newSecuredCR(core.Container{Name: config.ClickHouseBackupContainerName, SecurityContext: own}), backedUpCluster)
		require.Equal(t, own, requireBackupContainer(t, sts).SecurityContext)
	})
	t.Run("none to copy", func(t *testing.T) {
		sts, _ := createStatefulSet(t, normalize(t, newCR(newCluster(backedUpCluster, enabled()))), backedUpCluster)
		require.Nil(t, requireBackupContainer(t, sts).SecurityContext)
	})
}

// The sidecar takes the ClickHouse container's securityContext as is and never names a user of its own: a user
// left open is the platform's to assign, and an OpenShift SCC refuses any user outside the namespace's range.
func TestBackupSidecarNeverInventsAUser(t *testing.T) {
	newSecuredCR := func(pod *core.PodSecurityContext, clickHouse *core.SecurityContext, sidecar ...core.Container) *chi.ClickHouseInstallation {
		backup := enabled()
		if len(sidecar) > 0 {
			backup = enabledOn(podTemplateName)
		}
		cr := newCR(newCluster(backedUpCluster, backup))
		podSpec := &cr.Spec.Templates.PodTemplates[0].Spec
		podSpec.SecurityContext = pod
		podSpec.Containers[0].SecurityContext = clickHouse
		podSpec.Containers = append(podSpec.Containers, sidecar...)
		return normalize(t, cr)
	}
	nonRoot := func() *core.SecurityContext {
		return &core.SecurityContext{RunAsNonRoot: ptr.To(true), AllowPrivilegeEscalation: ptr.To(false)}
	}

	for name, tc := range map[string]struct {
		pod        *core.PodSecurityContext
		clickHouse *core.SecurityContext
		want       *core.SecurityContext
	}{
		"the ClickHouse container's lacks a user": {
			clickHouse: nonRoot(),
			want:       nonRoot(),
		},
		"the pod's lacks a user": {
			pod:  &core.PodSecurityContext{RunAsNonRoot: ptr.To(true)},
			want: nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			sts, _ := createStatefulSet(t, newSecuredCR(tc.pod, tc.clickHouse), backedUpCluster)
			require.Equal(t, tc.want, requireBackupContainer(t, sts).SecurityContext)
			clickHouse, _ := k8s.StatefulSetContainerGet(sts, config.ClickHouseContainerName)
			require.Equal(t, tc.clickHouse, clickHouse.SecurityContext, "the ClickHouse container keeps its own securityContext")
		})
	}

	t.Run("the base container's own is kept", func(t *testing.T) {
		sts, _ := createStatefulSet(t, newSecuredCR(nil, nonRoot(), core.Container{Name: config.ClickHouseBackupContainerName, SecurityContext: nonRoot()}), backedUpCluster)
		require.Equal(t, nonRoot(), requireBackupContainer(t, sts).SecurityContext)
	})
}

// Users are common for all clusters, yet enabling backup on one cluster must not change the StatefulSet of
// a host in another one: a changed pod template restarts the host. The backup user reaches ClickHouse
// through the users config, which is reloaded live, and nothing goes into the ClickHouse containers.
func TestBackupLeavesOtherClustersAlone(t *testing.T) {
	newPair := func(backup *chi.ClusterBackup) *chi.ClickHouseInstallation {
		return normalize(t, newCR(newCluster(plainCluster, nil), newCluster(backedUpCluster, backup)))
	}
	before := newPair(nil)
	after := newPair(enabled())
	require.True(t, after.Spec.Configuration.Users.Has(config.ClickHouseBackupUsername+"/password_sha256_hex"),
		"fixture must have the backup user in place")

	stsBefore, _ := createStatefulSet(t, before, plainCluster)
	stsAfter, _ := createStatefulSet(t, after, plainCluster)
	require.Equal(t, stsBefore, stsAfter, "enabling backup on another cluster changed this host's StatefulSet")

	backedUpBefore, _ := createStatefulSet(t, before, backedUpCluster)
	backedUpAfter, _ := createStatefulSet(t, after, backedUpCluster)
	clickHouseBefore, _ := k8s.StatefulSetContainerGet(backedUpBefore, config.ClickHouseContainerName)
	clickHouseAfter, _ := k8s.StatefulSetContainerGet(backedUpAfter, config.ClickHouseContainerName)
	require.Equal(t, clickHouseBefore, clickHouseAfter, "the ClickHouse container itself is not changed by the sidecar")
}

// Backup turned on without a storage, neither a storage with a Secret nor the pod template of the sidecar,
// stays off: its sidecar could never upload or download a backup, so the pods run as without backup - no sidecar,
// no port of its REST API, no backup user.
func TestBackupWithoutStorageAddsNothing(t *testing.T) {
	for name, backup := range map[string]*chi.ClusterBackup{
		"nothing named": {Enabled: types.NewStringBool(true)},
		"a storage of no Secret": {
			Enabled:  types.NewStringBool(true),
			Storages: []chi.BackupStorage{{Name: "main"}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			off := normalize(t, newCR(newCluster(backedUpCluster, nil)))
			lacking := normalize(t, newCR(newCluster(backedUpCluster, backup)))
			require.True(t, lacking.FindCluster(backedUpCluster).(*chi.Cluster).GetBackup().LacksStorage())
			require.False(t, lacking.IsBackupEnabled())
			require.False(t, lacking.Spec.Configuration.Users.Has(config.ClickHouseBackupUsername+"/password_sha256_hex"))

			stsOff, _ := createStatefulSet(t, off, backedUpCluster)
			stsLacking, _ := createStatefulSet(t, lacking, backedUpCluster)
			require.Equal(t, stsOff.Spec, stsLacking.Spec)
			_, ok := k8s.StatefulSetContainerGet(stsLacking, config.ClickHouseBackupContainerName)
			require.False(t, ok, "no sidecar")

			hostService := func(cr *chi.ClickHouseInstallation) *core.Service {
				return buildCreator(cr).CreateService(interfaces.ServiceHost, findHost(t, cr, backedUpCluster)).First()
			}
			require.Equal(t, hostService(off).Spec, hostService(lacking).Spec)
			require.NotContains(t, hostService(lacking).Spec.Ports, backupServicePort())
		})
	}
}

// The settings the backup Jobs rely on are env vars of the sidecar itself, which take precedence over the keys of
// the storage Secret. A schema restore is not ON CLUSTER, as one ON CLUSTER rewrites every Distributed table
// over a literal cluster other than the restored one - all-sharded, another cluster of the installation - to the
// restored cluster. Restored access objects are reloaded by SQL, as the default restart through systemctl fails in a
// sidecar, which clickhouse-backup ignores, leaving them unloaded. The REST API runs one operation at a time, as
// clickhouse-backup sets the env vars an operation overrides for the whole process.
func TestBackupSidecarSettingsForJobs(t *testing.T) {
	cr := newCR(newCluster(backedUpCluster, enabled()))
	cr.Spec.Backup = &chi.ClusterBackup{Storages: []chi.BackupStorage{{Name: "main", SecretRef: &core.LocalObjectReference{Name: storageSecret}}}}
	sts, _ := createStatefulSet(t, normalize(t, cr), backedUpCluster)
	sidecar := requireBackupContainer(t, sts)

	for name, want := range map[string]string{
		config.ClickHouseBackupEnvRestoreSchemaOnCluster:   "",
		config.ClickHouseBackupEnvClickHouseRestartCommand: "sql:SYSTEM RELOAD USERS",
		config.ClickHouseBackupEnvAPIAllowParallel:         "false",
		config.ClickHouseBackupEnvRestoreDatabaseMapping:   "",
		config.ClickHouseBackupEnvRestoreTableMapping:      "",
	} {
		e, ok := envVar(t, sidecar, name)
		require.True(t, ok, "%s must be set by the sidecar itself, or the storage Secret could set it", name)
		require.Equal(t, core.EnvVar{Name: name, Value: want}, e)
	}
}

// The configuration file defaults the paths of a storage that names none to a path of their own for every shard of
// every cluster, holding the macros clickhouse-backup expands on each host, see backupconfig. No env var of the
// sidecar sets a path: it would take precedence over the path the storage settings set.
func TestBackupSidecarDefaultPaths(t *testing.T) {
	cr := newCR(newCluster(backedUpCluster, enabled()))
	cr.Spec.Backup = &chi.ClusterBackup{Storages: []chi.BackupStorage{{Name: "main", SecretRef: &core.LocalObjectReference{Name: storageSecret}}}}
	sts, _ := createStatefulSet(t, normalize(t, cr), backedUpCluster)
	sidecar := requireBackupContainer(t, sts)

	require.Equal(t, sidecarCommand(), receivedCommand(sidecar))
	for _, e := range sidecar.Env {
		require.False(t, strings.HasSuffix(e.Name, "_PATH"), "the sidecar sets %s itself, over the storage settings", e.Name)
	}

	// Installations that share a storage keep their backups apart by default, those of one name in different
	// namespaces too, and so do their clusters and shards
	for _, path := range []string{chop.Config().Backup.StoragePath, chop.Config().Backup.StorageObjectDiskPath} {
		require.Contains(t, path, "{namespace}")
		require.Contains(t, path, "{installation}")
		require.Contains(t, path, "{cluster}")
		require.Contains(t, path, "{shard}")
	}
	// clickhouse-backup refuses a storage path that starts with the object disk path
	require.False(t, strings.HasPrefix(chop.Config().Backup.StoragePath, chop.Config().Backup.StorageObjectDiskPath))
	require.False(t, strings.HasPrefix(chop.Config().Backup.StorageObjectDiskPath, chop.Config().Backup.StoragePath))
}

// Every host defines the macros clickhouse-backup expands in a storage path, the namespace of its installation among
// them: a default path or one of a storage Secret can keep installations apart by namespace and name.
func TestHostMacrosHoldTheInstallationAndItsNamespace(t *testing.T) {
	cr := normalize(t, newCR(newCluster(backedUpCluster, enabled())))
	host := findHost(t, cr, backedUpCluster)
	files := managers.NewConfigFilesGenerator(managers.FilesGeneratorTypeClickHouse, cr, &config.GeneratorOptions{
		Settings: cr.GetSpecT().GetConfiguration().GetSettings(),
		Files:    cr.GetSpecT().GetConfiguration().GetFiles(),
	}).CreateConfigFiles(interfaces.FilesGroupHost, config.NewFilesGeneratorOptions().SetHost(host))
	var macros string
	for _, content := range files {
		if strings.Contains(content, "<macros>") {
			macros = content
		}
	}
	require.NotEmpty(t, macros, "fixture: the macros of the host")
	require.Contains(t, macros, "<installation>"+crName+"</installation>")
	require.Contains(t, macros, "<namespace>"+crNamespace+"</namespace>")
	require.Contains(t, macros, "<cluster>"+backedUpCluster+"</cluster>")
}

// The env of the pod template's container wins over what the operator sets - the operator adds no second entry
// of a name the container sets - save for what the Jobs rely on, see TestBackupSidecarPinsWhatTheJobsRelyOn.
func TestBackupSidecarEnvOverride(t *testing.T) {
	cr := newCR(newCluster(backedUpCluster, enabled()))
	sidecarOn(cr, cr.Spec.Configuration.Clusters[0], core.Container{Env: []core.EnvVar{
		{Name: config.ClickHouseBackupEnvClickHousePort, Value: "19000"},
		{Name: config.ClickHouseBackupEnvBackupsToKeepLocal, Value: "3"},
	}})
	cr = normalize(t, cr)

	sts, _ := createStatefulSet(t, cr, backedUpCluster)
	sidecar := requireBackupContainer(t, sts)
	require.Equal(t, "19000", envValue(t, sidecar, config.ClickHouseBackupEnvClickHousePort))
	require.Equal(t, "3", envValue(t, sidecar, config.ClickHouseBackupEnvBackupsToKeepLocal))
}

// What the backup Jobs rely on is set by the operator over the pod template's own container, each once, and over the
// configuration file, which env vars take precedence over: ON CLUSTER schema restores on every replica would rewrite Distributed tables,
// restored access entities left unloaded would be missing, parallel operations would see each other's
// settings, and a restore mapped to other databases or tables would drop what the guard of its Job never
// checked. The other settings stay the user's.
func TestBackupSidecarPinsWhatTheJobsRelyOn(t *testing.T) {
	overrides := []core.EnvVar{
		{Name: config.ClickHouseBackupEnvRestoreSchemaOnCluster, Value: "{cluster}"},
		{Name: config.ClickHouseBackupEnvClickHouseRestartCommand, Value: "exec:systemctl restart clickhouse-server"},
		{Name: config.ClickHouseBackupEnvAPIAllowParallel, Value: "true"},
		{Name: config.ClickHouseBackupEnvRestoreDatabaseMapping, Value: "db:restored_db"},
		{Name: config.ClickHouseBackupEnvRestoreTableMapping, Value: "t:restored_t"},
		{Name: config.ClickHouseBackupEnvAPIListen, Value: "127.0.0.1:7171"},
		{Name: config.ClickHouseBackupEnvAPISecure, ValueFrom: &core.EnvVarSource{ConfigMapKeyRef: &core.ConfigMapKeySelector{
			LocalObjectReference: core.LocalObjectReference{Name: "api"}, Key: "secure",
		}}},
		{Name: config.ClickHouseBackupSecretKeyAPIUsername, Value: "admin"},
		{Name: config.ClickHouseBackupSecretKeyAPIPassword, Value: "admin"},
	}
	pinned := map[string]string{
		config.ClickHouseBackupEnvAPIListen:                "0.0.0.0:" + strconv.Itoa(config.ClickHouseBackupAPIPortNumber),
		config.ClickHouseBackupEnvAPISecure:                "false",
		config.ClickHouseBackupEnvRestoreSchemaOnCluster:   "",
		config.ClickHouseBackupEnvClickHouseRestartCommand: config.ClickHouseBackupRestartCommandReloadUsers,
		config.ClickHouseBackupEnvAPIAllowParallel:         "false",
		config.ClickHouseBackupEnvRestoreDatabaseMapping:   "",
		config.ClickHouseBackupEnvRestoreTableMapping:      "",
	}
	for name, setUp := range map[string]func(cr *chi.ClickHouseInstallation, backup *chi.ClusterBackup){
		"the pod template's container": func(cr *chi.ClickHouseInstallation, backup *chi.ClusterBackup) {
			backup.PodTemplate = types.NewString(podTemplateName)
			podSpec := &cr.Spec.Templates.PodTemplates[0].Spec
			podSpec.Containers = append(podSpec.Containers, core.Container{
				Name: config.ClickHouseBackupContainerName,
				Env:  append(slices.Clone(overrides), overrides...),
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			backup := enabled()
			cr := newCR(newCluster(backedUpCluster, backup))
			setUp(cr, backup)
			sts, _ := createStatefulSet(t, normalize(t, cr), backedUpCluster)
			sidecar := requireBackupContainer(t, sts)
			for name, want := range pinned {
				require.Equal(t, want, envValue(t, sidecar, name), name)
			}
			for _, key := range []string{config.ClickHouseBackupSecretKeyAPIUsername, config.ClickHouseBackupSecretKeyAPIPassword} {
				_, ok := envVar(t, sidecar, key)
				require.False(t, ok, "%s would override the credentials of the configuration file, which the Jobs call the API with", key)
			}
		})
	}
}

// A secure-only host serves no plain TCP, so the sidecar talks TLS to the secure port, without verifying
// a certificate that is not issued for localhost.
func TestBackupSidecarSecureOnlyHost(t *testing.T) {
	cluster := newCluster(backedUpCluster, enabled())
	cluster.Secure = types.NewStringBool(true)
	cluster.Insecure = types.NewStringBool(false)
	cr := normalize(t, newCR(cluster))

	sts, host := createStatefulSet(t, cr, backedUpCluster)
	require.False(t, types.IsPortAssigned(host.TCPPort.Value()), "fixture must yield a host without plain TCP")
	require.True(t, types.IsPortAssigned(host.TLSPort.Value()))

	sidecar := requireBackupContainer(t, sts)
	require.Equal(t, strconv.Itoa(int(host.TLSPort.Value())), envValue(t, sidecar, config.ClickHouseBackupEnvClickHousePort))
	require.Equal(t, "true", envValue(t, sidecar, config.ClickHouseBackupEnvClickHouseSecure))
	require.Equal(t, "true", envValue(t, sidecar, config.ClickHouseBackupEnvClickHouseSkipVerify))
}

// A container of the sidecar's name in the pod template is the base of the sidecar: what it sets is kept,
// what it lacks is filled in. In a cluster without backup it is the user's container and stays untouched.
func TestBackupSidecarFromUserContainer(t *testing.T) {
	const (
		userImage     = "registry.example.com/clickhouse-backup:custom"
		userPort      = "19000"
		userMountPath = "/scratch"
	)
	userContainer := core.Container{
		Name:         config.ClickHouseBackupContainerName,
		Image:        userImage,
		Env:          []core.EnvVar{{Name: config.ClickHouseBackupEnvClickHousePort, Value: userPort}},
		VolumeMounts: []core.VolumeMount{{Name: "scratch", MountPath: userMountPath}},
	}
	cr := newCR(newCluster(backedUpCluster, enabledOn(podTemplateName)), newCluster(plainCluster, nil))
	podSpec := &cr.Spec.Templates.PodTemplates[0].Spec
	podSpec.Containers = append(podSpec.Containers, userContainer)
	podSpec.Volumes = []core.Volume{{Name: "scratch", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}}}
	cr = normalize(t, cr)

	sts, _ := createStatefulSet(t, cr, backedUpCluster)
	sidecar := requireBackupContainer(t, sts)
	require.Equal(t, userImage, sidecar.Image, "the user's image is kept")
	require.Equal(t, sidecarCommand(), receivedCommand(sidecar), "a missing command is filled in")
	require.Equal(t, userPort, envValue(t, sidecar, config.ClickHouseBackupEnvClickHousePort), "the user's env var is kept")
	require.Equal(t, config.ClickHouseBackupClickHouseHost, envValue(t, sidecar, config.ClickHouseBackupEnvClickHouseHost), "a missing env var is filled in")
	_, ok := envVar(t, sidecar, config.ClickHouseBackupSecretKeyAPIPassword)
	require.False(t, ok, "the credentials are in the configuration file")
	requireConfigVolume(t, sts, backedUpCluster, true)
	require.Contains(t, sidecar.Ports, core.ContainerPort{Name: config.ClickHouseBackupAPIPortName, ContainerPort: config.ClickHouseBackupAPIPortNumber})
	requireMountedAt(t, sidecar, userMountPath)
	requireMountedAt(t, sidecar, diskMountPath)

	plain, _ := createStatefulSet(t, cr, plainCluster)
	untouched := requireBackupContainer(t, plain)
	require.Equal(t, userImage, untouched.Image)
	require.Empty(t, untouched.Command)
	require.Equal(t, userContainer.Env, untouched.Env, "backup disabled - the user's container gets no backup env")
	require.Empty(t, untouched.EnvFrom)
	require.Empty(t, untouched.Ports)
}

func findVolume(sts *apps.StatefulSet, name string) (core.Volume, bool) {
	for _, volume := range sts.Spec.Template.Spec.Volumes {
		if volume.Name == name {
			return volume, true
		}
	}
	return core.Volume{}, false
}

// The sidecar reads its configuration file from the Secret of its cluster, mounted at the directory clickhouse-backup
// reads it from, whatever the pod template's own container mounts or sets: a mount there or below or above, a volume
// of the name of the operator's, and env vars that would override the file, credentials or the file itself, give way.
// A base container's own command or args make the Secret mandatory, as another command might serve the REST API to
// everybody without the credentials the file holds, and the operator's command waits for the file.
func TestBackupSidecarConfigFile(t *testing.T) {
	const ownVolume = "own"
	emptyDir := core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}
	for name, tc := range map[string]struct {
		base *core.Container
		// volumes are the pod template's own
		volumes []core.Volume
		// clickHouseMounts are the ClickHouse container's own
		clickHouseMounts []core.VolumeMount
		wantMandatory    bool
	}{
		"nothing of its own": {},
		"mount at the directory": {
			base:    &core.Container{VolumeMounts: []core.VolumeMount{{Name: ownVolume, MountPath: config.DirPathClickHouseBackupConfig}}},
			volumes: []core.Volume{{Name: ownVolume, VolumeSource: emptyDir}},
		},
		"mount of the file": {
			base: &core.Container{VolumeMounts: []core.VolumeMount{{
				Name: ownVolume, MountPath: path.Join(config.DirPathClickHouseBackupConfig, backupconfig.FileName), SubPath: backupconfig.FileName,
			}}},
			volumes: []core.Volume{{Name: ownVolume, VolumeSource: emptyDir}},
		},
		"mount above the directory": {
			base:    &core.Container{VolumeMounts: []core.VolumeMount{{Name: ownVolume, MountPath: path.Dir(config.DirPathClickHouseBackupConfig)}}},
			volumes: []core.Volume{{Name: ownVolume, VolumeSource: emptyDir}},
		},
		"ClickHouse mounts the directory": {
			clickHouseMounts: []core.VolumeMount{{Name: ownVolume, MountPath: config.DirPathClickHouseBackupConfig}},
			volumes:          []core.Volume{{Name: ownVolume, VolumeSource: emptyDir}},
		},
		"volume of the operator's name": {
			volumes: []core.Volume{{Name: config.ClickHouseBackupConfigVolumeName, VolumeSource: emptyDir}},
		},
		"env vars over the file": {
			base: &core.Container{Env: []core.EnvVar{
				{Name: config.ClickHouseBackupSecretKeyAPIUsername, Value: "admin"},
				{Name: config.ClickHouseBackupSecretKeyAPIPassword, Value: "secret"},
				{Name: config.ClickHouseBackupSecretKeyClickHouseUsername, Value: "default"},
				k8s.NewEnvVarFromSecret(config.ClickHouseBackupSecretKeyClickHousePassword, backupSecretName, config.ClickHouseBackupSecretKeyClickHousePassword),
				{Name: config.ClickHouseBackupEnvConfig, Value: "/etc/elsewhere/config.yml"},
			}},
		},
		"own command": {
			base:          &core.Container{Command: []string{config.ClickHouseBackupCommand, config.ClickHouseBackupCommandServer, "--watch"}},
			wantMandatory: true,
		},
		"own args": {
			base:          &core.Container{Args: []string{config.ClickHouseBackupCommandServer}},
			wantMandatory: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			backup := enabledOn(podTemplateName)
			cr := newCR(newCluster(backedUpCluster, backup))
			podSpec := &cr.Spec.Templates.PodTemplates[0].Spec
			base := tc.base.DeepCopy()
			if base == nil {
				base = &core.Container{}
			}
			base.Name = config.ClickHouseBackupContainerName
			podSpec.Containers = append(podSpec.Containers, *base)
			podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, tc.clickHouseMounts...)
			podSpec.Volumes = append(podSpec.Volumes, tc.volumes...)
			sts, _ := createStatefulSet(t, normalize(t, cr), backedUpCluster)
			sidecar := requireBackupContainer(t, sts)

			requireConfigVolume(t, sts, backedUpCluster, !tc.wantMandatory)
			paths := map[string]bool{}
			for _, mount := range sidecar.VolumeMounts {
				require.False(t, paths[mount.MountPath], "two mounts at %s", mount.MountPath)
				paths[mount.MountPath] = true
				if mount.Name != config.ClickHouseBackupConfigVolumeName {
					require.False(t, strings.HasPrefix(config.DirPathClickHouseBackupConfig, mount.MountPath) ||
						strings.HasPrefix(mount.MountPath, config.DirPathClickHouseBackupConfig),
						"the sidecar mounts %s at %s, over its configuration file", mount.Name, mount.MountPath)
				}
			}
			for _, key := range []string{
				config.ClickHouseBackupSecretKeyAPIUsername,
				config.ClickHouseBackupSecretKeyAPIPassword,
				config.ClickHouseBackupSecretKeyClickHouseUsername,
				config.ClickHouseBackupSecretKeyClickHousePassword,
				config.ClickHouseBackupEnvConfig,
			} {
				_, ok := envVar(t, sidecar, key)
				require.False(t, ok, "env var %s would override the configuration file", key)
			}
			require.Nil(t, sidecar.LivenessProbe)
		})
	}
}

// rewordComments rewords every comment of the script and adds a comment and blank lines, as an operator upgrade that
// changes no code of the script would
func rewordComments(t *testing.T, script string) string {
	t.Helper()
	lines := strings.Split(script, "\n")
	for i, line := range lines {
		if trimmed := strings.TrimSpace(line); strings.HasPrefix(trimmed, "#") {
			lines[i] = strings.TrimSuffix(line, trimmed) + "# reworded"
		}
	}
	reworded := "# added\n\n" + strings.Join(lines, "\n\n")
	require.NotEqual(t, script, reworded)
	return reworded
}

// The pod template carries the sidecar's script without its comments: a changed pod template rolls the pods,
// restarting ClickHouse, which an operator upgrade that merely rewords a comment of a script must not.
func TestBackupSidecarScriptCommentsLeavePodTemplateAlone(t *testing.T) {
	cr := newCR(newCluster(backedUpCluster, enabled()))
	cr.Spec.Backup = &chi.ClusterBackup{Storages: []chi.BackupStorage{{Name: "main", SecretRef: &core.LocalObjectReference{Name: storageSecret}}}}
	cr = normalize(t, cr)
	shipped, _ := createStatefulSet(t, cr, backedUpCluster)

	restore := chiCreator.ShipBackupScript(rewordComments(t, chiCreator.BackupSidecarScriptSource))
	defer restore()
	reworded, _ := createStatefulSet(t, cr, backedUpCluster)
	require.Equal(t, shipped.Spec.Template, reworded.Spec.Template)
}

func backupServicePort() core.ServicePort {
	return core.ServicePort{
		Name:       config.ClickHouseBackupAPIPortName,
		Protocol:   core.ProtocolTCP,
		Port:       config.ClickHouseBackupAPIPortNumber,
		TargetPort: intstr.FromInt32(config.ClickHouseBackupAPIPortNumber),
	}
}

// The host Service exposes the sidecar's REST API for hosts of backup-enabled clusters - also when it is
// built from the user's service template, which itself stays as written.
func TestBackupHostServicePort(t *testing.T) {
	const hostServiceTemplate = "host-svc"
	templatePorts := []core.ServicePort{{Name: chi.ChDefaultTCPPortName, Port: chi.ChDefaultTCPPortNumber}}

	templated := newCluster("templated", enabled())
	templated.Templates = &chi.TemplatesList{ReplicaServiceTemplate: hostServiceTemplate}
	cr := newCR(newCluster(backedUpCluster, enabled()), newCluster(plainCluster, nil), templated)
	cr.Spec.Templates.ServiceTemplates = []chi.ServiceTemplate{{
		Name: hostServiceTemplate,
		Spec: core.ServiceSpec{Ports: templatePorts},
	}}
	cr = normalize(t, cr)
	creator := buildCreator(cr)

	hostService := func(cluster string) *core.Service {
		svc := creator.CreateService(interfaces.ServiceHost, findHost(t, cr, cluster)).First()
		require.NotNil(t, svc)
		return svc
	}

	require.Contains(t, hostService(backedUpCluster).Spec.Ports, backupServicePort())
	require.NotContains(t, hostService(plainCluster).Spec.Ports, backupServicePort())

	fromTemplate := hostService("templated")
	require.Contains(t, fromTemplate.Spec.Ports, backupServicePort())
	require.Contains(t, fromTemplate.Spec.Ports, templatePorts[0])
	template, ok := cr.GetServiceTemplate(hostServiceTemplate)
	require.True(t, ok)
	require.Equal(t, templatePorts, template.Spec.Ports, "the service template of the installation must not be modified")
}

// The REST API is plain HTTP, so it is published only by host Services reachable from inside the cluster
// alone - ClusterIP, headless or not, without external IPs. A host Service reachable from outside does not get
// the port.
func TestBackupHostServicePortClusterInternalOnly(t *testing.T) {
	externalIPs := []string{"203.0.113.10"}
	for name, tc := range map[string]struct {
		spec core.ServiceSpec
		want bool
	}{
		"type not set":            {spec: core.ServiceSpec{}, want: true},
		"ClusterIP":               {spec: core.ServiceSpec{Type: core.ServiceTypeClusterIP}, want: true},
		"headless":                {spec: core.ServiceSpec{Type: core.ServiceTypeClusterIP, ClusterIP: core.ClusterIPNone}, want: true},
		"NodePort":                {spec: core.ServiceSpec{Type: core.ServiceTypeNodePort}, want: false},
		"LoadBalancer":            {spec: core.ServiceSpec{Type: core.ServiceTypeLoadBalancer}, want: false},
		"ExternalName":            {spec: core.ServiceSpec{Type: core.ServiceTypeExternalName, ExternalName: "ch.example.com"}, want: false},
		"ClusterIP, external IPs": {spec: core.ServiceSpec{Type: core.ServiceTypeClusterIP, ExternalIPs: externalIPs}, want: false},
		"no type, external IPs":   {spec: core.ServiceSpec{ExternalIPs: externalIPs}, want: false},
		"user's own port":         {spec: core.ServiceSpec{Ports: []core.ServicePort{{Name: "my-backup", Port: config.ClickHouseBackupAPIPortNumber}}}, want: true},
	} {
		t.Run(name, func(t *testing.T) {
			const hostServiceTemplate = "host-svc"
			cluster := newCluster(backedUpCluster, enabled())
			cluster.Templates = &chi.TemplatesList{ReplicaServiceTemplate: hostServiceTemplate}
			cr := newCR(cluster)
			cr.Spec.Templates.ServiceTemplates = []chi.ServiceTemplate{{Name: hostServiceTemplate, Spec: tc.spec}}
			cr = normalize(t, cr)

			svc := buildCreator(cr).CreateService(interfaces.ServiceHost, findHost(t, cr, backedUpCluster)).First()
			require.Equal(t, tc.want, chiCreator.ServiceHasBackupPort(svc))
			if !tc.want {
				require.NotContains(t, svc.Spec.Ports, backupServicePort())
			}
		})
	}
	require.False(t, chiCreator.ServiceHasBackupPort(nil))
}

// Backup annotations steer the operator. Stamped on the installation they must not reach the pod template
// or the StatefulSet - a change there would restart every host - while other annotations still do.
func TestBackupAnnotationsNotPropagated(t *testing.T) {
	const teamAnnotation = "example.com/team"
	cr := newCR(newCluster(backedUpCluster, enabled()))
	cr.Annotations = map[string]string{teamAnnotation: "dwh"}
	for _, annotation := range chi.BackupAnnotations {
		cr.Annotations[annotation] = "nightly"
	}
	cr = normalize(t, cr)
	for _, annotation := range chi.BackupAnnotations {
		require.Contains(t, cr.GetAnnotations(), annotation, "the installation itself keeps its backup annotations")
	}

	sts, host := createStatefulSet(t, cr, backedUpCluster)
	svc := buildCreator(cr).CreateService(interfaces.ServiceHost, host).First()
	require.NotNil(t, svc)
	for what, annotations := range map[string]map[string]string{
		"StatefulSet":  sts.Annotations,
		"pod template": sts.Spec.Template.Annotations,
		"host Service": svc.Annotations,
	} {
		require.Equal(t, "dwh", annotations[teamAnnotation], "%s must still carry the installation's annotations", what)
		for _, annotation := range chi.BackupAnnotations {
			require.NotContains(t, annotations, annotation, "%s must not carry %s", what, annotation)
		}
	}
}

// Kubelet expands the command of a container, reducing "$$" to "$": the sidecar script, which reads the environment
// from /proc/$$/environ, reaches the container as written only escaped.
func TestBackupSidecarScriptSurvivesKubeletExpansion(t *testing.T) {
	require.Contains(t, chiCreator.BackupSidecarScript, "$$", "the script relies on $$")
	cr := newCR(newCluster(backedUpCluster, enabled()))
	cr.Spec.Backup = &chi.ClusterBackup{Storages: []chi.BackupStorage{{Name: "main", SecretRef: &core.LocalObjectReference{Name: storageSecret}}}}
	sts, _ := createStatefulSet(t, normalize(t, cr), backedUpCluster)
	sidecar := requireBackupContainer(t, sts)
	require.Equal(t, sidecarCommand(), receivedCommand(sidecar))
	require.NotEqual(t, sidecarCommand(), sidecar.Command, "an unescaped script would lose its $$")
}

// The sidecar may be built on a pod template of its own, apart from the one the pods run: only its clickhouse-backup
// container is taken, not its other containers or its volumes, and ClickHouse stays as the pods' pod template has it.
func TestBackupSidecarFromItsOwnPodTemplate(t *testing.T) {
	const (
		backupTemplate = "backup"
		backupImage    = "registry.example.com/clickhouse-backup:own"
	)
	cr := newCR(newCluster(backedUpCluster, enabledOn(backupTemplate)))
	cr.Spec.Templates.PodTemplates = append(cr.Spec.Templates.PodTemplates, chi.PodTemplate{
		Name: backupTemplate,
		Spec: core.PodSpec{
			Containers: []core.Container{
				{Name: "other", Image: "registry.example.com/other:1"},
				{Name: config.ClickHouseBackupContainerName, Image: backupImage, Env: []core.EnvVar{{Name: "LOG_LEVEL", Value: "debug"}}},
			},
			Volumes: []core.Volume{{Name: "unused", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}}},
		},
	})
	sts, _ := createStatefulSet(t, normalize(t, cr), backedUpCluster)
	sidecar := requireBackupContainer(t, sts)
	require.Equal(t, backupImage, sidecar.Image)
	require.Equal(t, "debug", envValue(t, sidecar, "LOG_LEVEL"))
	clickHouse, ok := k8s.StatefulSetContainerGet(sts, config.ClickHouseContainerName)
	require.True(t, ok)
	require.Equal(t, serverImage, clickHouse.Image)
	_, other := k8s.StatefulSetContainerGet(sts, "other")
	require.False(t, other, "only the clickhouse-backup container of the backup's pod template is taken")
	_, unused := findVolume(sts, "unused")
	require.False(t, unused, "the volumes of the backup's pod template are not taken")
}

// The operator's sidecar is never taken for the ClickHouse container, wherever a pod template lists it: a template
// holding nothing but the sidecar gets the default ClickHouse container, and one that lists the sidecar ahead of a
// ClickHouse container named otherwise gets ClickHouse's probes on that container, and no second server.
func TestBackupSidecarIsNeverClickHouse(t *testing.T) {
	t.Run("the sidecar alone", func(t *testing.T) {
		cr := newCR(newCluster(backedUpCluster, enabledOn(podTemplateName)))
		cr.Spec.Templates.PodTemplates[0].Spec.Containers = []core.Container{{Name: config.ClickHouseBackupContainerName}}
		sts, _ := createStatefulSet(t, normalize(t, cr), backedUpCluster)
		clickHouse, ok := k8s.StatefulSetContainerGet(sts, config.ClickHouseContainerName)
		require.True(t, ok, "no ClickHouse container")
		require.Equal(t, config.DefaultClickHouseDockerImage, clickHouse.Image)
		sidecar := requireBackupContainer(t, sts)
		require.Equal(t, config.DefaultClickHouseBackupDockerImage, sidecar.Image)
		require.Nil(t, sidecar.ReadinessProbe)
	})
	t.Run("the sidecar ahead of ClickHouse", func(t *testing.T) {
		const clickHousePod = "clickhouse-pod"
		cr := newCR(newCluster(backedUpCluster, enabledOn(podTemplateName)))
		cr.Spec.Templates.PodTemplates[0].Spec.Containers = []core.Container{
			{Name: config.ClickHouseBackupContainerName},
			{Name: clickHousePod, Image: "clickhouse/clickhouse-server:25.8"},
		}
		sts, _ := createStatefulSet(t, normalize(t, cr), backedUpCluster)
		var names []string
		for i := range sts.Spec.Template.Spec.Containers {
			names = append(names, sts.Spec.Template.Spec.Containers[i].Name)
		}
		require.Equal(t, []string{config.ClickHouseBackupContainerName, clickHousePod}, names, "no second ClickHouse container")
		clickHouse, _ := k8s.StatefulSetContainerGet(sts, clickHousePod)
		require.NotNil(t, clickHouse.ReadinessProbe, "ClickHouse gets its probes")
		require.Nil(t, requireBackupContainer(t, sts).ReadinessProbe, "the sidecar gets none of ClickHouse's")
	})
}

// The operator gives the sidecar no envFrom source: the storage settings are in the configuration file. The
// container's own sources stay as they are.
func TestBackupSidecarTakesNoEnvFrom(t *testing.T) {
	configMap := core.EnvFromSource{ConfigMapRef: &core.ConfigMapEnvSource{LocalObjectReference: core.LocalObjectReference{Name: "other"}}}
	for name, own := range map[string][]core.EnvFromSource{
		"none of the container's own": nil,
		"the container's own":         {configMap},
	} {
		t.Run(name, func(t *testing.T) {
			cr := newCR(newCluster(backedUpCluster, enabled()))
			if own != nil {
				sidecarOn(cr, cr.Spec.Configuration.Clusters[0], core.Container{EnvFrom: own})
			}
			sts, _ := createStatefulSet(t, normalize(t, cr), backedUpCluster)
			require.Equal(t, own, requireBackupContainer(t, sts).EnvFrom)
		})
	}
}
