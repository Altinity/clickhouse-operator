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

package creator

import (
	_ "embed"
	"net"
	"path"
	"slices"
	"strconv"
	"strings"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/backupconfig"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// backupSidecarScriptSource starts the clickhouse-backup server of the sidecar, as its command
//
//go:embed scripts/backup-sidecar.sh
var backupSidecarScriptSource string

// backupSidecarScript is the script as the pod template carries it. Its comments are left out: a changed pod
// template rolls the pods, restarting ClickHouse, which a reworded comment must not.
var backupSidecarScript = stripScriptComments(backupSidecarScriptSource)

// stripScriptComments drops the lines of the shell script that hold nothing but a comment or blanks, keeping a
// shebang. A '#' within a line of code stays. The scripts hold no heredoc and no string spanning lines, whose lines
// would be dropped alike.
func stripScriptComments(script string) string {
	var kept []string
	for i, line := range strings.Split(script, "\n") {
		trimmed := strings.TrimSpace(line)
		shebang := (i == 0) && strings.HasPrefix(trimmed, "#!")
		if (trimmed == "") || (strings.HasPrefix(trimmed, "#") && !shebang) {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n") + "\n"
}

// backupPinnedEnvVarNames are the env vars of the sidecar the backup Jobs rely on. The operator sets them over what
// the base container sets, and they take precedence over the configuration file: the Jobs call the REST API over
// plain HTTP on its port, a restore that ran ON CLUSTER on every replica would rewrite Distributed tables, restored
// access entities left unloaded would be missing, operations running alongside in one sidecar would see each other's
// settings, and a restore mapped to other databases or tables would drop what the guard of its Job never checked.
var backupPinnedEnvVarNames = []string{
	config.ClickHouseBackupEnvAPIListen,
	config.ClickHouseBackupEnvAPISecure,
	config.ClickHouseBackupEnvAPIAllowParallel,
	config.ClickHouseBackupEnvRestoreSchemaOnCluster,
	config.ClickHouseBackupEnvClickHouseRestartCommand,
	config.ClickHouseBackupEnvRestoreDatabaseMapping,
	config.ClickHouseBackupEnvRestoreTableMapping,
}

// backupConfigFileEnvVarNames are the env vars the base container must not set: the credentials, which the
// configuration file the operator generates holds and an env var would override, and the variable that would make
// clickhouse-backup read another configuration file instead.
var backupConfigFileEnvVarNames = []string{
	config.ClickHouseBackupSecretKeyAPIUsername,
	config.ClickHouseBackupSecretKeyAPIPassword,
	config.ClickHouseBackupSecretKeyClickHouseUsername,
	config.ClickHouseBackupSecretKeyClickHousePassword,
	config.ClickHouseBackupEnvConfig,
}

// IsBackupSidecarCommand reports whether the sidecar runs the operator's command, which waits for its configuration
// file before it starts the server, nil-safe
func IsBackupSidecarCommand(container *core.Container) bool {
	return (container != nil) && (len(container.Args) == 0) && slices.Equal(container.Command, newBackupSidecarCommand())
}

// newBackupSidecarCommand is the operator's command of the sidecar, for a base container that runs none of its own
func newBackupSidecarCommand() []string {
	return k8s.EscapeVariableReferencesAll(newBackupScriptCommand(backupSidecarScript,
		path.Join(config.DirPathClickHouseBackupConfig, backupconfig.FileName),
		path.Join(config.DirPathClickHouseBackupConfig, backupconfig.APIFingerprintFileName),
		config.ClickHouseBackupCommand, config.ClickHouseBackupCommandServer))
}

// newBackupScriptCommand runs the script with bash, which the backup image has for the backup Jobs as well. The
// arguments become the script's positional parameters, with the container name as its $0.
func newBackupScriptCommand(script string, args ...string) []string {
	return append([]string{"bash", "-c", script, config.ClickHouseBackupContainerName}, args...)
}

// getContainerClickHouseBackup
func (cm *ContainerManager) getContainerClickHouseBackup(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return k8s.StatefulSetContainerGet(statefulSet, config.ClickHouseBackupContainerName)
}

// NewBackupContainer builds the clickhouse-backup sidecar of a host of a backup-enabled cluster - as its pod runs it,
// as the backup Jobs take their image and identity from it, and as the storage path check reads it. The container of
// that name of the pod template the cluster's backup names is the base: what it sets is kept and only what it
// lacks is filled in, the settings the Jobs rely on last. Hosts of other clusters get none, whatever their pod
// template holds.
//
// The sidecar reads its configuration file, which holds the credentials and the storage settings, from the Secret
// the operator generates for the cluster, mounted where clickhouse-backup looks for it. The operator's command
// waits for the file, so a sidecar started before the Secret exists serves nothing until then. Any other command
// might serve the REST API to everybody without the file, so it gets the Secret as mandatory.
func (cm *ContainerManager) NewBackupContainer(host *chi.Host) (core.Container, bool) {
	backup := host.GetCluster().GetBackup()
	if !backup.IsEnabled() {
		return core.Container{}, false
	}

	container := core.Container{Name: config.ClickHouseBackupContainerName}
	if backupTemplate, ok := host.GetBackupPodTemplate(); ok {
		if base, ok := k8s.PodSpecContainerGet(&backupTemplate.Spec, config.ClickHouseBackupContainerName); ok {
			container = *base.DeepCopy()
		}
	}
	var clickHouse *core.Container
	if podTemplate, ok := host.GetPodTemplate(); ok {
		clickHouse, _ = PodSpecContainerGetClickHouse(&podTemplate.Spec)
	}

	if container.Image == "" {
		container.Image = config.DefaultClickHouseBackupDockerImage
	}
	if (len(container.Command) == 0) && (len(container.Args) == 0) {
		container.Command = newBackupSidecarCommand()
	}
	container.Env = slices.DeleteFunc(container.Env, func(envVar core.EnvVar) bool {
		return slices.Contains(backupConfigFileEnvVarNames, envVar.Name)
	})
	envVars := newBackupEnvVars(host)
	container.Env = util.MergeEnvVars(container.Env, envVars...)
	container.Env = util.OverwriteEnvVars(container.Env, slices.DeleteFunc(envVars, func(envVar core.EnvVar) bool {
		return !slices.Contains(backupPinnedEnvVarNames, envVar.Name)
	})...)
	containerEnsureBackupConfigMount(&container)
	// A sidecar without a securityContext gets the pod rejected wherever a policy demands one of every container.
	// It runs as ClickHouse does then, which also gives it the same access to the server's files. The copy is
	// taken as is: a user it leaves open is the platform's to assign, as an OpenShift SCC does from a range that
	// refuses any fixed one - where the platform does not, the base container names the user.
	if (container.SecurityContext == nil) && (clickHouse != nil) {
		container.SecurityContext = clickHouse.SecurityContext.DeepCopy()
	}
	k8s.ContainerEnsurePortByName(&container, config.ClickHouseBackupAPIPortName, config.ClickHouseBackupAPIPortNumber)
	return container, true
}

// containerEnsureBackupConfigMount mounts the configuration file Secret where clickhouse-backup reads its
// configuration file from, in place of whatever the base container mounts there or below. The whole directory is
// mounted, never the file by subPath, as kubelet keeps a file mounted by subPath as it was when the container started.
func containerEnsureBackupConfigMount(container *core.Container) {
	container.VolumeMounts = slices.DeleteFunc(container.VolumeMounts, func(mount core.VolumeMount) bool {
		return isBackupConfigPath(mount.MountPath)
	})
	mount := k8s.CreateVolumeMount(config.ClickHouseBackupConfigVolumeName, config.DirPathClickHouseBackupConfig)
	mount.ReadOnly = true
	container.VolumeMounts = append(container.VolumeMounts, mount)
}

// isBackupConfigPath reports whether a mount at the path would hide the configuration file of the sidecar, or put
// files of its own beside it: the path is the directory of the file, lies within it, or holds it
func isBackupConfigPath(mountPath string) bool {
	return isPathWithin(mountPath, config.DirPathClickHouseBackupConfig) || isPathWithin(config.DirPathClickHouseBackupConfig, mountPath)
}

// ensureContainerSpecifiedClickHouseBackup puts the clickhouse-backup sidecar NewBackupContainer builds into pods
// of a backup-enabled cluster, in place of the pod template's container of that name, if it has one, along with the
// volume of its configuration file
func (cm *ContainerManager) ensureContainerSpecifiedClickHouseBackup(statefulSet *apps.StatefulSet, host *chi.Host) {
	sidecar, ok := cm.NewBackupContainer(host)
	if !ok {
		return
	}
	if container, ok := cm.getContainerClickHouseBackup(statefulSet); ok {
		*container = sidecar
	} else {
		k8s.PodSpecAddContainer(&statefulSet.Spec.Template.Spec, sidecar)
	}
	cm.statefulSetEnsureBackupConfigVolume(statefulSet, host, IsBackupSidecarCommand(&sidecar))
}

// statefulSetEnsureBackupConfigVolume gives the pod the volume of the configuration file Secret of the host's
// cluster, in place of a volume of that name the pod template has. Only the file and the fingerprint of the
// credentials of the REST API are projected: the Secret holds the settings the storage path check reads as well,
// which the backup Jobs take as env vars.
func (cm *ContainerManager) statefulSetEnsureBackupConfigVolume(statefulSet *apps.StatefulSet, host *chi.Host, optional bool) {
	volume := core.Volume{
		Name: config.ClickHouseBackupConfigVolumeName,
		VolumeSource: core.VolumeSource{
			Secret: &core.SecretVolumeSource{
				SecretName: cm.namer.Name(interfaces.NameBackupConfigSecret, host.GetCR(), host.GetCluster().GetName()),
				Items: []core.KeyToPath{
					{Key: backupconfig.FileName, Path: backupconfig.FileName},
					{Key: backupconfig.APIFingerprintFileName, Path: backupconfig.APIFingerprintFileName},
				},
				Optional: ptr.To(optional),
			},
		},
	}
	volumes := statefulSet.Spec.Template.Spec.Volumes
	if index := slices.IndexFunc(volumes, func(v core.Volume) bool { return v.Name == volume.Name }); index >= 0 {
		volumes[index] = volume
		return
	}
	k8s.StatefulSetAppendVolumes(statefulSet, volume)
}

// ensureContainerVolumeMountsClickHouseBackup mounts every volume of the ClickHouse container into the
// clickhouse-backup sidecar as well, as the sidecar works on all disks of the server directly
func (cm *ContainerManager) ensureContainerVolumeMountsClickHouseBackup(statefulSet *apps.StatefulSet, host *chi.Host) {
	if !host.GetCluster().GetBackup().IsEnabled() {
		return
	}
	backup, ok := cm.getContainerClickHouseBackup(statefulSet)
	if !ok {
		return
	}
	clickHouse, ok := cm.getContainerClickHouse(statefulSet)
	if !ok {
		return
	}
	statefulSetEnsureDataStorageVolume(statefulSet, clickHouse)
	containerCopyVolumeMounts(backup, clickHouse)
}

// containerCopyVolumeMounts gives the container every mount of the source container exactly as it is. One volume
// may be mounted several times - at different paths or of different subPaths - and each such mount counts. A path
// the container mounts something at already keeps that mount, as two mounts at one path make the pod invalid, and
// nothing is mounted where it would touch the configuration file of the sidecar.
func containerCopyVolumeMounts(container, source *core.Container) {
	for i := range source.VolumeMounts {
		mount := source.VolumeMounts[i]
		taken := slices.ContainsFunc(container.VolumeMounts, func(existing core.VolumeMount) bool {
			return existing.MountPath == mount.MountPath
		})
		if !k8s.VolumeMountIsValid(mount) || taken || isBackupConfigPath(mount.MountPath) {
			continue
		}
		container.VolumeMounts = append(container.VolumeMounts, *mount.DeepCopy())
	}
}

// statefulSetEnsureDataStorageVolume gives the ClickHouse container a volume at its data path, unless a volume
// is mounted there or above already. Without one the data lives in the container's own filesystem, where the
// sidecar cannot reach it: FREEZE leaves its shadow copy and a restore leaves the parts to attach on the other
// side. An emptyDir outlives a restart of the container, which the container's filesystem does not, yet not the
// deletion of the pod - data meant to persist needs a volume claim template.
func statefulSetEnsureDataStorageVolume(statefulSet *apps.StatefulSet, clickHouse *core.Container) {
	for i := range clickHouse.VolumeMounts {
		if isPathWithin(config.DirPathDataStorage, clickHouse.VolumeMounts[i].MountPath) {
			return
		}
	}
	if !k8s.StatefulSetHasVolumeByName(statefulSet, config.ClickHouseBackupSharedDataVolumeName) {
		k8s.StatefulSetAppendVolumes(statefulSet, core.Volume{
			Name:         config.ClickHouseBackupSharedDataVolumeName,
			VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}},
		})
	}
	k8s.ContainerAppendVolumeMount(clickHouse, k8s.CreateVolumeMount(config.ClickHouseBackupSharedDataVolumeName, config.DirPathDataStorage))
}

// isPathWithin reports whether target is dir itself or lies below it
func isPathWithin(target, dir string) bool {
	target = path.Clean(target)
	dir = path.Clean(dir)
	return (dir == "/") || (target == dir) || strings.HasPrefix(target, dir+"/")
}

// newBackupEnvVars builds the environment the operator provides to the clickhouse-backup sidecar: what is the same
// for as long as the pod runs, as kubelet resolves env vars only when it starts a container. The credentials and the
// storage settings, which may change meanwhile, are in the configuration file instead.
func newBackupEnvVars(host *chi.Host) []core.EnvVar {
	envVars := []core.EnvVar{
		{
			Name:  config.ClickHouseBackupEnvAPIListen,
			Value: net.JoinHostPort(config.ClickHouseBackupAPIListenHost, strconv.Itoa(config.ClickHouseBackupAPIPortNumber)),
		},
		// The backup Jobs call the REST API over plain HTTP, see backupPinnedEnvVarNames
		{
			Name:  config.ClickHouseBackupEnvAPISecure,
			Value: strconv.FormatBool(false),
		},
		{
			Name:  config.ClickHouseBackupEnvAPIAllowParallel,
			Value: strconv.FormatBool(false),
		},
		{
			Name:  config.ClickHouseBackupEnvClickHouseHost,
			Value: config.ClickHouseBackupClickHouseHost,
		},
		// Set, and empty, so that nothing else sets it, see backupPinnedEnvVarNames: a schema restore ON CLUSTER
		// rewrites every Distributed table over a literal cluster other than the restored one - all-sharded,
		// another cluster of the installation - to the restored cluster. The backup Jobs restore the schema on
		// every replica instead, and run ON CLUSTER only where they ask for it themselves.
		{
			Name:  config.ClickHouseBackupEnvRestoreSchemaOnCluster,
			Value: "",
		},
		{
			Name:  config.ClickHouseBackupEnvClickHouseRestartCommand,
			Value: config.ClickHouseBackupRestartCommandReloadUsers,
		},
		// Set, and empty, so that nothing else sets them, see backupPinnedEnvVarNames
		{
			Name:  config.ClickHouseBackupEnvRestoreDatabaseMapping,
			Value: "",
		},
		{
			Name:  config.ClickHouseBackupEnvRestoreTableMapping,
			Value: "",
		},
		{
			Name:  config.ClickHouseBackupEnvBackupsToKeepLocal,
			Value: config.ClickHouseBackupBackupsToKeepLocalDeleteAfterUpload,
		},
		{
			Name:  config.ClickHouseBackupEnvAllowEmptyBackups,
			Value: strconv.FormatBool(true),
		},
	}

	port := host.TCPPort.Value()
	if !types.IsPortAssigned(port) && types.IsPortAssigned(host.TLSPort.Value()) {
		// A secure-only host serves TCP over TLS alone. Its certificate is issued for the host's
		// names rather than for localhost, so the sidecar does not verify it.
		port = host.TLSPort.Value()
		envVars = append(envVars,
			core.EnvVar{
				Name:  config.ClickHouseBackupEnvClickHouseSecure,
				Value: strconv.FormatBool(true),
			},
			core.EnvVar{
				Name:  config.ClickHouseBackupEnvClickHouseSkipVerify,
				Value: strconv.FormatBool(true),
			},
		)
	}
	if types.IsPortAssigned(port) {
		envVars = append(envVars, core.EnvVar{
			Name:  config.ClickHouseBackupEnvClickHousePort,
			Value: strconv.Itoa(int(port)),
		})
	}

	return envVars
}

// ServiceHasBackupPort reports whether the host Service publishes the clickhouse-backup REST API the backup
// Jobs call, nil-safe
func ServiceHasBackupPort(svc *core.Service) bool {
	if svc == nil {
		return false
	}
	return serviceSpecHasBackupPort(&svc.Spec)
}

// serviceSpecIsClusterInternal reports whether the Service is reachable from inside the cluster only - a ClusterIP
// one, headless or not, ClusterIP being the type of a Service that names none - with no external IPs, which
// publish the ports of even a ClusterIP Service outside the cluster
func serviceSpecIsClusterInternal(spec *core.ServiceSpec) bool {
	return ((spec.Type == "") || (spec.Type == core.ServiceTypeClusterIP)) && (len(spec.ExternalIPs) == 0)
}

func serviceSpecHasBackupPort(spec *core.ServiceSpec) bool {
	return slices.ContainsFunc(spec.Ports, func(port core.ServicePort) bool {
		return (port.Name == config.ClickHouseBackupAPIPortName) || (port.Port == config.ClickHouseBackupAPIPortNumber)
	})
}

// serviceSpecEnsureBackupPort exposes the clickhouse-backup REST API of the host, which the backup Jobs call.
// The API is plain HTTP behind basic auth, so a Service reachable from outside the cluster does not get it.
func serviceSpecEnsureBackupPort(spec *core.ServiceSpec) {
	if !serviceSpecIsClusterInternal(spec) || serviceSpecHasBackupPort(spec) {
		return
	}
	spec.Ports = append(spec.Ports, core.ServicePort{
		Name:       config.ClickHouseBackupAPIPortName,
		Protocol:   core.ProtocolTCP,
		Port:       config.ClickHouseBackupAPIPortNumber,
		TargetPort: intstr.FromInt32(config.ClickHouseBackupAPIPortNumber),
	})
}
