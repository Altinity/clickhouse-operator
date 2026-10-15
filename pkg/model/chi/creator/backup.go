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
	"regexp"
	"slices"
	"strconv"
	"strings"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Timing of the sidecar's liveness probe. kubelet resolves the env vars of a container when it starts the container,
// and updates the files of a mounted Secret when it syncs the pod, which it does every minute or so. A sidecar
// started between a change of a Secret and that sync runs with env vars newer than the files meanwhile, so the probe
// fails for longer than that before kubelet restarts the sidecar.
const (
	backupLivenessPeriodSeconds    = 30
	backupLivenessFailureThreshold = 4
	// backupLivenessAPITimeoutSeconds bounds the probe's call of the REST API, which the probe's own timeout leaves
	// room for
	backupLivenessAPITimeoutSeconds = 5
	backupLivenessTimeoutSeconds    = 2 * backupLivenessAPITimeoutSeconds
	// backupLivenessQuietSeconds is how long after an operation the probe restarts no sidecar, so that a backup Job,
	// polling every few seconds, sees how its step ended and starts its next one first
	backupLivenessQuietSeconds = 60
)

// backupSidecarScriptSource starts the clickhouse-backup server of the sidecar, as its command
//
//go:embed scripts/backup-sidecar.sh
var backupSidecarScriptSource string

// backupSidecarLivenessScriptSource is the liveness probe that restarts the sidecar once a Secret it takes env vars
// from changes
//
//go:embed scripts/backup-sidecar-liveness.sh
var backupSidecarLivenessScriptSource string

// The scripts as the pod template carries them. Their comments are left out: a changed pod template rolls the pods,
// restarting ClickHouse, which a reworded comment must not.
var (
	backupSidecarScript         = stripScriptComments(backupSidecarScriptSource)
	backupSidecarLivenessScript = stripScriptComments(backupSidecarLivenessScriptSource)
)

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
// the base container sets: the Jobs call the REST API over plain HTTP on its port with the operator's
// credentials, a restore that ran ON CLUSTER on every replica would rewrite Distributed tables, restored access
// entities left unloaded would be missing, operations running alongside in one sidecar would see each other's
// settings, and a restore mapped to other databases or tables would drop what the guard of its Job never checked.
var backupPinnedEnvVarNames = []string{
	config.ClickHouseBackupEnvAPIListen,
	config.ClickHouseBackupEnvAPISecure,
	config.ClickHouseBackupSecretKeyAPIUsername,
	config.ClickHouseBackupSecretKeyAPIPassword,
	config.ClickHouseBackupEnvAPIAllowParallel,
	config.ClickHouseBackupEnvRestoreSchemaOnCluster,
	config.ClickHouseBackupEnvClickHouseRestartCommand,
	config.ClickHouseBackupEnvRestoreDatabaseMapping,
	config.ClickHouseBackupEnvRestoreTableMapping,
}

// IsBackupSidecarCommand reports whether the sidecar runs the operator's command, which defaults the storage paths
// its environment leaves unset, nil-safe
func IsBackupSidecarCommand(container *core.Container) bool {
	return (container != nil) && (len(container.Args) == 0) && slices.Equal(container.Command, newBackupSidecarCommand())
}

// newBackupSidecarCommand is the operator's command of the sidecar, for a base container that runs none of its own
func newBackupSidecarCommand() []string {
	return k8s.EscapeVariableReferencesAll(newBackupScriptCommand(backupSidecarScript,
		append(newBackupSidecarArgs(), config.ClickHouseBackupCommand, config.ClickHouseBackupCommandServer)...))
}

// envVarIdentifierRE matches the env var names the liveness probe compares, as IDENTIFIER_RE of its script does
var envVarIdentifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

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
// The operator's command serves the REST API only with its credentials, so these may be missing: a sidecar started
// while the backup Secret is missing idles, leaving its pod ready, rather than failing to start. Any other command
// might serve the API to everybody without them, so it gets them as mandatory.
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
	ownCommand := (len(container.Command) == 0) && (len(container.Args) == 0)
	if ownCommand {
		container.Command = newBackupSidecarCommand()
	}
	envVars := cm.newBackupEnvVars(host, ownCommand)
	container.Env = util.MergeEnvVars(container.Env, envVars...)
	if secret := backup.GetStorageSecretName(); secret != "" {
		containerEnsureEnvFromSecret(&container, secret)
	}
	container.Env = util.OverwriteEnvVars(container.Env, slices.DeleteFunc(envVars, func(envVar core.EnvVar) bool {
		return !slices.Contains(backupPinnedEnvVarNames, envVar.Name)
	})...)
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

// ensureContainerSpecifiedClickHouseBackup puts the clickhouse-backup sidecar NewBackupContainer builds into pods
// of a backup-enabled cluster, in place of the pod template's container of that name, if it has one
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
	container, _ := cm.getContainerClickHouseBackup(statefulSet)
	cm.ensureContainerSecretsFollowedClickHouseBackup(statefulSet, container, host)
}

// ensureContainerSecretsFollowedClickHouseBackup makes the sidecar follow the Secrets it takes env vars from - the
// backup Secret and the storage Secret - without restarting ClickHouse. kubelet resolves env vars only when
// it starts a container, but keeps the files of a mounted Secret up to date: the sidecar gets both Secrets mounted,
// and a liveness probe that fails once their files hold values the sidecar did not start with, so that kubelet
// restarts the sidecar alone. The volumes are optional, as the env vars are, and a missing Secret leaves its
// directory empty. A liveness probe the base container has is kept, and nothing is mounted for it then.
func (cm *ContainerManager) ensureContainerSecretsFollowedClickHouseBackup(
	statefulSet *apps.StatefulSet,
	container *core.Container,
	host *chi.Host,
) {
	if container.LivenessProbe != nil {
		return
	}

	credentials := ""
	backupSecret := cm.namer.Name(interfaces.NameBackupAutoSecret, host.GetCR())
	if items := containerSecretEnvVarItems(container, backupSecret); len(items) > 0 {
		source := &core.SecretVolumeSource{SecretName: backupSecret, Items: items, Optional: ptr.To(true)}
		if statefulSetMountSecret(statefulSet, container, config.ClickHouseBackupCredentialsVolumeName, config.DirPathClickHouseBackupCredentials, source) {
			credentials = config.DirPathClickHouseBackupCredentials
		}
	}

	storage := ""
	var explicit []string
	if secret := host.GetCluster().GetBackup().GetStorageSecretName(); (secret != "") && containerTakesSecretLast(container, secret) {
		source := &core.SecretVolumeSource{SecretName: secret, Optional: ptr.To(true)}
		if statefulSetMountSecret(statefulSet, container, config.ClickHouseBackupStorageVolumeName, config.DirPathClickHouseBackupStorage, source) {
			storage = config.DirPathClickHouseBackupStorage
			// The container's own env vars take precedence over every key of the Secret
			for i := range container.Env {
				explicit = append(explicit, container.Env[i].Name)
			}
		}
	}

	if (credentials == "") && (storage == "") {
		return
	}
	container.LivenessProbe = &core.Probe{
		ProbeHandler: core.ProbeHandler{
			Exec: &core.ExecAction{
				Command: k8s.EscapeVariableReferencesAll(newBackupScriptCommand(backupSidecarLivenessScript, append(newBackupLivenessArgs(credentials, storage), explicit...)...)),
			},
		},
		TimeoutSeconds:   backupLivenessTimeoutSeconds,
		PeriodSeconds:    backupLivenessPeriodSeconds,
		FailureThreshold: backupLivenessFailureThreshold,
	}
}

// newBackupLivenessArgs are the arguments of the liveness probe ahead of the env vars the sidecar sets itself: its
// timing, and the mounts of the two Secrets, "" for one that is not followed
func newBackupLivenessArgs(credentials, storage string) []string {
	return []string{
		strconv.Itoa(backupLivenessAPITimeoutSeconds),
		strconv.Itoa(backupLivenessQuietSeconds),
		credentials,
		storage,
	}
}

// newBackupSidecarArgs are the arguments of the sidecar's command ahead of the command it starts: the paths on the
// storage it defaults to. They are no env vars of the container, which would take precedence over the
// paths the storage Secret sets, and hide a change of these from the liveness probe.
func newBackupSidecarArgs() []string {
	return []string{
		chop.Config().Backup.StoragePath,
		chop.Config().Backup.StorageObjectDiskPath,
	}
}

// newBackupScriptCommand runs the script with bash, which the backup image has for the backup Jobs as well. The
// arguments become the script's positional parameters, with the container name as its $0.
func newBackupScriptCommand(script string, args ...string) []string {
	return append([]string{"bash", "-c", script, config.ClickHouseBackupContainerName}, args...)
}

// containerSecretEnvVarItems maps every env var of the container that takes its value from a key of the Secret
// to a file named after the env var, which is how the liveness probe tells the env var a file feeds. Of an env var
// named twice the last entry counts, as for kubelet. A name that is no shell identifier is left out: the probe
// does not compare it, and it might be no valid file name either.
func containerSecretEnvVarItems(container *core.Container, secretName string) []core.KeyToPath {
	var items []core.KeyToPath
	for i := range container.Env {
		envVar := &container.Env[i]
		items = slices.DeleteFunc(items, func(item core.KeyToPath) bool {
			return item.Path == envVar.Name
		})
		if (envVar.ValueFrom == nil) || (envVar.ValueFrom.SecretKeyRef == nil) {
			continue
		}
		if (envVar.ValueFrom.SecretKeyRef.Name != secretName) || !envVarIdentifierRE.MatchString(envVar.Name) {
			continue
		}
		items = append(items, core.KeyToPath{Key: envVar.ValueFrom.SecretKeyRef.Key, Path: envVar.Name})
	}
	return items
}

// containerTakesSecretLast reports whether every key of the Secret becomes an env var of the container named after
// the key: the Secret is the container's last envFrom source, which wins over the others, and adds no prefix
func containerTakesSecretLast(container *core.Container, secretName string) bool {
	if len(container.EnvFrom) == 0 {
		return false
	}
	last := container.EnvFrom[len(container.EnvFrom)-1]
	return (last.SecretRef != nil) && (last.SecretRef.Name == secretName) && (last.Prefix == "")
}

// statefulSetMountSecret mounts the Secret read-only into the container alone, reporting whether it did. Where the
// pod has a volume of that name already, or the container a mount at that path, the files there are not the
// Secret's, and nothing is mounted.
func statefulSetMountSecret(
	statefulSet *apps.StatefulSet,
	container *core.Container,
	volumeName string,
	mountPath string,
	source *core.SecretVolumeSource,
) bool {
	taken := slices.ContainsFunc(container.VolumeMounts, func(mount core.VolumeMount) bool {
		return mount.MountPath == mountPath
	})
	if taken || k8s.StatefulSetHasVolumeByName(statefulSet, volumeName) {
		return false
	}
	k8s.StatefulSetAppendVolumes(statefulSet, core.Volume{
		Name:         volumeName,
		VolumeSource: core.VolumeSource{Secret: source},
	})
	mount := k8s.CreateVolumeMount(volumeName, mountPath)
	mount.ReadOnly = true
	container.VolumeMounts = append(container.VolumeMounts, mount)
	return true
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
// the container mounts something at already keeps that mount, as two mounts at one path make the pod invalid.
func containerCopyVolumeMounts(container, source *core.Container) {
	for i := range source.VolumeMounts {
		mount := source.VolumeMounts[i]
		taken := slices.ContainsFunc(container.VolumeMounts, func(existing core.VolumeMount) bool {
			return existing.MountPath == mount.MountPath
		})
		if !k8s.VolumeMountIsValid(mount) || taken {
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

// newBackupEnvVars builds the environment the operator provides to the clickhouse-backup sidecar. The sidecar's
// ClickHouse password comes from the same Secret the normalizer derives the backup user's password from. The
// credentials are optional only where the sidecar runs the operator's command, see NewBackupContainer.
func (cm *ContainerManager) newBackupEnvVars(host *chi.Host, optionalCredentials bool) []core.EnvVar {
	secretName := cm.namer.Name(interfaces.NameBackupAutoSecret, host.GetCR())
	credential := func(key string) core.EnvVar {
		envVar := k8s.NewEnvVarFromSecret(key, secretName, key)
		if optionalCredentials {
			envVar.ValueFrom.SecretKeyRef.Optional = ptr.To(true)
		}
		return envVar
	}
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
		credential(config.ClickHouseBackupSecretKeyAPIUsername),
		credential(config.ClickHouseBackupSecretKeyAPIPassword),
		credential(config.ClickHouseBackupSecretKeyClickHouseUsername),
		credential(config.ClickHouseBackupSecretKeyClickHousePassword),
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

// containerEnsureEnvFromSecret ensures the container takes all keys of the Secret as env vars, unprefixed and over
// every other envFrom source: the Secret is its last one. A source of the container's own that names the Secret
// elsewhere, or with a prefix, stays as it is - kubelet takes repeated sources alike. The Secret is optional: a
// missing one would keep the container from being created, and a pod that never gets ready stalls the reconcile of
// the whole CR - an optional sidecar must not take ClickHouse down with it.
func containerEnsureEnvFromSecret(container *core.Container, secretName string) {
	if containerTakesSecretLast(container, secretName) {
		return
	}
	container.EnvFrom = append(container.EnvFrom, core.EnvFromSource{
		SecretRef: &core.SecretEnvSource{
			LocalObjectReference: core.LocalObjectReference{
				Name: secretName,
			},
			Optional: ptr.To(true),
		},
	})
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
