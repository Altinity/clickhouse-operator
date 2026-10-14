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
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// kubeObjects are the Secrets and ConfigMaps of a namespace, as kubelet resolves env vars from them.
type kubeObjects struct {
	secrets    map[string]map[string]string
	configMaps map[string]map[string]string
}

var kubeEnvReferenceRegexp = regexp.MustCompile(`\$\(([A-Za-z_][A-Za-z0-9_]*)\)`)

// resolve returns the environment kubelet starts a container with: envFrom, then env in order. A value
// refers to the variables set before it; an optional source without the key leaves the variable as it
// receivedCommand returns a container command as kubelet hands it to the container: $(VAR_NAME) references to env
// expanded and "$$" reduced to "$".
func receivedCommand(command []string, env map[string]string) []string {
	lookup := func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	}
	var received []string
	for _, arg := range command {
		received = append(received, k8s.ExpandVariableReferences(arg, lookup))
	}
	return received
}

// was; a field of the pod resolves to podName.
func (k kubeObjects) resolve(t *testing.T, container *core.Container, podName string) map[string]string {
	t.Helper()
	env := map[string]string{}
	lookup := func(secret, configMap *core.LocalObjectReference, optional *bool) (map[string]string, bool) {
		var data map[string]string
		var found bool
		if secret != nil {
			data, found = k.secrets[secret.Name]
		} else {
			data, found = k.configMaps[configMap.Name]
		}
		require.True(t, found || ptr.Deref(optional, false), "container %s: a source that is not optional is missing", container.Name)
		return data, found
	}
	for _, from := range container.EnvFrom {
		var data map[string]string
		if from.SecretRef != nil {
			data, _ = lookup(&from.SecretRef.LocalObjectReference, nil, from.SecretRef.Optional)
		} else {
			data, _ = lookup(nil, &from.ConfigMapRef.LocalObjectReference, from.ConfigMapRef.Optional)
		}
		for key, value := range data {
			env[from.Prefix+key] = value
		}
	}
	for _, e := range container.Env {
		source := e.ValueFrom
		switch {
		case source == nil:
			env[e.Name] = kubeEnvReferenceRegexp.ReplaceAllStringFunc(e.Value, func(reference string) string {
				if value, set := env[reference[2:len(reference)-1]]; set {
					return value
				}
				return reference
			})
		case source.FieldRef != nil:
			env[e.Name] = podName
		case source.SecretKeyRef != nil:
			data, _ := lookup(&source.SecretKeyRef.LocalObjectReference, nil, source.SecretKeyRef.Optional)
			if value, found := data[source.SecretKeyRef.Key]; found {
				env[e.Name] = value
			} else {
				require.True(t, ptr.Deref(source.SecretKeyRef.Optional, false), "container %s: %s from a missing key", container.Name, e.Name)
			}
		case source.ConfigMapKeyRef != nil:
			data, _ := lookup(nil, &source.ConfigMapKeyRef.LocalObjectReference, source.ConfigMapKeyRef.Optional)
			if value, found := data[source.ConfigMapKeyRef.Key]; found {
				env[e.Name] = value
			} else {
				require.True(t, ptr.Deref(source.ConfigMapKeyRef.Optional, false), "container %s: %s from a missing key", container.Name, e.Name)
			}
		default:
			t.Fatalf("container %s: unsupported source of %s", container.Name, e.Name)
		}
	}
	return env
}

// sidecarOfHost is what the clickhouse-backup sidecar of a host starts the server with.
type sidecarOfHost struct {
	cluster, service string
	env              map[string]string
}

// runSidecars builds the StatefulSet of every backup-enabled host as the operator does, resolves the
// env of its sidecar as kubelet does, and runs the sidecar's command with the server replaced by
// `env -0`: what the server would start with, the paths the operator's command defaults included.
func runSidecars(t *testing.T, inMemory *api.ClickHouseInstallation, kube kubeObjects) []sidecarOfHost {
	t.Helper()
	creator := productionCreator(inMemory)
	names := managers.NewNameManager(managers.NameManagerTypeClickHouse)
	var sidecars []sidecarOfHost
	inMemory.WalkHosts(func(host *api.Host) error {
		if !host.GetCluster().GetBackup().IsEnabled() {
			return nil
		}
		statefulSet := creator.CreateStatefulSet(host, false)
		container, ok := k8s.StatefulSetContainerGet(statefulSet, config.ClickHouseBackupContainerName)
		require.True(t, ok)
		env := kube.resolve(t, container, names.Name(interfaces.NameStatefulSet, host)+"-0")
		command := []string{"env", "-0"}
		if received := receivedCommand(container.Command, env); slices.Equal(received[len(received)-2:], []string{config.ClickHouseBackupCommand, config.ClickHouseBackupCommandServer}) {
			command = append(slices.Clone(received[:len(received)-2]), command...)
		}
		run := exec.Command(command[0], command[1:]...)
		run.Env = []string{"PATH=" + os.Getenv("PATH")}
		for name, value := range env {
			run.Env = append(run.Env, name+"="+value)
		}
		out, err := run.Output()
		require.NoError(t, err, string(out))
		started := map[string]string{}
		for _, entry := range strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00") {
			name, value, _ := strings.Cut(entry, "=")
			started[name] = value
		}
		sidecars = append(sidecars, sidecarOfHost{
			cluster: host.Runtime.Address.ClusterName,
			service: names.Name(interfaces.NameStatefulSetService, host),
			env:     started,
		})
		return nil
	})
	return sidecars
}

// providers maps the storage types to the prefix of their settings.
var providers = map[string]string{"s3": "S3", "gcs": "GCS", "azblob": "AZBLOB", "cos": "COS", "ftp": "FTP", "sftp": "SFTP"}

// jobSetting returns a setting of the cluster at index as the path check of a Job reads it, the defaults
// of the storage paths applied as the sidecars apply them, and whether it is set.
func jobSetting(env map[string]string, index int, key string) (string, bool) {
	if value, set := env[pathClusterEnv(index, key)]; set {
		return value, true
	}
	prefix := providers[env[pathClusterEnv(index, "REMOTE_STORAGE")]]
	for suffix, def := range map[string]string{"_PATH": envDefaultPath, "_OBJECT_DISK_PATH": envDefaultObjectDiskPath} {
		if (prefix == "") || (key != prefix+suffix) {
			continue
		}
		if value, set := env[pathClusterEnv(index, def)]; set {
			return value, value != ""
		}
		value, set := env[def]
		return value, set
	}
	return "", false
}

// sidecarSettingsInstallation has a cluster whose sidecars take their settings from its pod template's
// clickhouse-backup container, a value and a Secret key, over the installation's storage Secret; one without backup; one whose
// sidecars run a command of their own from a pod template, with a Secret of its own; and warm, which
// inherits everything, and which the cases change.
func sidecarSettingsInstallation(t *testing.T, mutate func(chi *api.ClickHouseInstallation, warm *api.Cluster)) (inMemory, stored *api.ClickHouseInstallation) {
	t.Helper()
	hot := layoutCluster("hot", 2, 2, &api.ClusterBackup{Enabled: types.NewStringBool(true)})
	plain := layoutCluster("plain", 1, 1, &api.ClusterBackup{Enabled: types.NewStringBool(false)})
	cold := layoutCluster("cold", 1, 2, &api.ClusterBackup{Enabled: types.NewStringBool(true), Storages: []api.BackupStorage{{Name: "main", SecretRef: &core.LocalObjectReference{Name: "s3-cold"}}}})
	// warm takes the storage Secret of the installation
	warm := layoutCluster("warm", 2, 1, &api.ClusterBackup{Enabled: types.NewStringBool(true)})
	chi := newCHI("demo", hot, plain, cold, warm)
	chi.Spec.Backup = &api.ClusterBackup{Storages: []api.BackupStorage{{Name: "main", SecretRef: &core.LocalObjectReference{Name: "s3-main"}}}}
	chi.Spec.Templates = &api.Templates{PodTemplates: []api.PodTemplate{{
		Name: "own-command",
		Spec: core.PodSpec{Containers: []core.Container{
			{Name: config.ClickHouseContainerName, Image: "clickhouse/clickhouse-server"},
			{Name: config.ClickHouseBackupContainerName, Image: "registry/backup", Command: []string{"clickhouse-backup", "server"}},
		}},
	}}}
	sidecarTemplate(chi, "hot-env", core.Container{Env: []core.EnvVar{
		{Name: "S3_ENDPOINT", Value: "http://minio:9000"},
		{Name: "S3_BUCKET", ValueFrom: &core.EnvVarSource{SecretKeyRef: &core.SecretKeySelector{
			LocalObjectReference: core.LocalObjectReference{Name: "bucket"}, Key: "name",
		}}},
	}})
	useTemplate(chi, hot, "hot-env")
	useTemplate(chi, cold, "own-command")
	if mutate != nil {
		mutate(chi, warm)
	}
	return normalize(t, chi)
}

// sidecarTemplate adds a pod template whose clickhouse-backup container is the one given.
func sidecarTemplate(chi *api.ClickHouseInstallation, name string, sidecar core.Container) {
	sidecar.Name = config.ClickHouseBackupContainerName
	chi.Spec.Templates.PodTemplates = append(chi.Spec.Templates.PodTemplates, api.PodTemplate{
		Name: name,
		Spec: core.PodSpec{Containers: []core.Container{{Name: config.ClickHouseContainerName, Image: "clickhouse/clickhouse-server"}, sidecar}},
	})
}

// useTemplate gives the cluster the named pod template, which has to be added already. One that holds a
// clickhouse-backup container is the one the cluster's backup names as well, as no other pod template of a
// backup-enabled cluster may hold that container.
func useTemplate(chi *api.ClickHouseInstallation, cluster *api.Cluster, name string) {
	cluster.Templates = &api.TemplatesList{PodTemplate: name}
	templates := chi.GetSpecT().GetTemplates().GetPodTemplates()
	for i := range templates {
		if templates[i].Name != name {
			continue
		}
		if _, ok := k8s.PodSpecContainerGet(&templates[i].Spec, config.ClickHouseBackupContainerName); ok {
			if cluster.Backup == nil {
				cluster.Backup = &api.ClusterBackup{}
			}
			cluster.Backup.PodTemplate = types.NewString(name)
		}
	}
}

// sidecarEnv gives warm a pod template whose clickhouse-backup container sets the env given.
func sidecarEnv(chi *api.ClickHouseInstallation, warm *api.Cluster, env []core.EnvVar) {
	sidecarTemplate(chi, "own-env", core.Container{Env: env})
	useTemplate(chi, warm, "own-env")
}

// clickHouseTemplate adds a pod template whose ClickHouse container mounts the given volume, and no
// clickhouse-backup container.
func clickHouseTemplate(chi *api.ClickHouseInstallation, name string, mount core.VolumeMount) {
	chi.Spec.Templates.PodTemplates = append(chi.Spec.Templates.PodTemplates, api.PodTemplate{
		Name: name,
		Spec: core.PodSpec{
			Containers: []core.Container{{
				Name:         config.ClickHouseContainerName,
				Image:        "clickhouse/clickhouse-server",
				VolumeMounts: []core.VolumeMount{mount},
			}},
			Volumes: []core.Volume{{Name: mount.Name, VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}}},
		},
	})
}

// commandTemplate adds a pod template whose clickhouse-backup container runs the given command line.
func commandTemplate(chi *api.ClickHouseInstallation, name string, command, args []string) {
	sidecarTemplate(chi, name, core.Container{Image: "registry/backup", Command: command, Args: args})
}

// optionalSecretKey and optionalConfigMapKey take a value from an optional key of the named Secret or ConfigMap.
func optionalSecretKey(name, key string) *core.EnvVarSource {
	return &core.EnvVarSource{SecretKeyRef: &core.SecretKeySelector{
		LocalObjectReference: core.LocalObjectReference{Name: name}, Key: key, Optional: ptr.To(true),
	}}
}

func optionalConfigMapKey(name, key string) *core.EnvVarSource {
	return &core.EnvVarSource{ConfigMapKeyRef: &core.ConfigMapKeySelector{
		LocalObjectReference: core.LocalObjectReference{Name: name}, Key: key, Optional: ptr.To(true),
	}}
}

func pathTemplateEnv(values ...string) []core.EnvVar {
	var env []core.EnvVar
	for _, value := range values {
		env = append(env, core.EnvVar{Name: "S3_PATH", Value: value})
	}
	return env
}

// The path check of a Job sees every setting it reads of every backup-enabled cluster as the sidecars of
// the cluster start with it, wherever they take it from - the storage Secret, the pod template's
// own clickhouse-backup container, the defaults of the operator's command - as kubelet
// resolves both. Where it cannot, the cluster is passed as unverifiable, with why: the Job must never
// check paths the sidecars do not use.
func TestJobSeesTheSettingsTheSidecarsRunWith(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate       func(chi *api.ClickHouseInstallation, warm *api.Cluster)
		unverifiable string
	}{
		"settings of the Secret, the pod template's container and the operator's defaults": {},
		"a path of the pod template's container": {mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
			sidecarTemplate(chi, "shared", core.Container{Env: pathTemplateEnv("shared/{shard}")})
			useTemplate(chi, warm, "shared")
		}},
		"a path the pod template's container sets twice": {mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
			sidecarTemplate(chi, "twice", core.Container{Env: pathTemplateEnv("first/{shard}", "last/{shard}")})
			useTemplate(chi, warm, "twice")
		}},
		"a path from an optional key a Secret lacks": {mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
			sidecarEnv(chi, warm, []core.EnvVar{{Name: "S3_PATH", ValueFrom: optionalSecretKey("bucket", "path")}})
		}},
		"a path from an optional key of another Secret over the storage Secret": {mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
			sidecarEnv(chi, warm, []core.EnvVar{{Name: "S3_PATH", ValueFrom: optionalSecretKey("own-paths", "path")}})
		}},
		"a path from an optional key of a ConfigMap over the storage Secret": {mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
			sidecarEnv(chi, warm, []core.EnvVar{{Name: "S3_PATH", ValueFrom: optionalConfigMapKey("paths", "path")}})
		}},
		"a path from an optional key a ConfigMap lacks": {mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
			sidecarEnv(chi, warm, []core.EnvVar{{Name: "S3_PATH", ValueFrom: optionalConfigMapKey("paths", "none")}})
		}},
		"a path of the pod template's container, then one from an optional key a ConfigMap lacks": {mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
			sidecarTemplate(chi, "value-then-missing", core.Container{Env: []core.EnvVar{
				{Name: "S3_PATH", Value: "template/{cluster}/{shard}"},
				{Name: "S3_PATH", ValueFrom: optionalConfigMapKey("paths", "none")},
			}})
			useTemplate(chi, warm, "value-then-missing")
		}},
		"paths of the pod template's container from optional keys, the last one missing": {mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
			sidecarTemplate(chi, "present-then-missing", core.Container{Env: []core.EnvVar{
				{Name: "S3_PATH", ValueFrom: optionalConfigMapKey("paths", "path")},
				{Name: "S3_PATH", ValueFrom: optionalSecretKey("own-paths", "none")},
			}})
			useTemplate(chi, warm, "present-then-missing")
		}},
		"a path that refers to another variable, replaced by a plain one": {mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
			sidecarTemplate(chi, "reference-then-value", core.Container{Env: []core.EnvVar{
				{Name: "S3_PATH", Value: "backup/$(HOSTNAME)"},
				{Name: "S3_PATH", Value: "template/{cluster}/{shard}"},
			}})
			useTemplate(chi, warm, "reference-then-value")
		}},
		"a pod template's container with a command of its own on every host": {mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
			useTemplate(chi, warm, "own-command")
		}},
		"hosts that differ in settings no check reads": {mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
			clickHouseTemplate(chi, "other", core.VolumeMount{Name: "scratch", MountPath: "/scratch"})
			warm.Layout = &api.ChiClusterLayout{Shards: []*api.ChiShard{
				{Name: "0", ReplicasCount: 1, Templates: &api.TemplatesList{PodTemplate: "other"}},
				{Name: "1", ReplicasCount: 1},
			}}
		}},
		"envFrom of the pod template's container": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				sidecarTemplate(chi, "env-from", core.Container{EnvFrom: []core.EnvFromSource{{SecretRef: &core.SecretEnvSource{
					LocalObjectReference: core.LocalObjectReference{Name: "paths"},
				}}}})
				useTemplate(chi, warm, "env-from")
			},
			unverifiable: "the clickhouse-backup container of pod template env-from takes environment variables from envFrom",
		},
		// A prefix makes other names of the keys, of which REMOTE_ + STORAGE is one the check reads
		"the storage Secret with a prefix in the pod template's container": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				sidecarTemplate(chi, "env-from", core.Container{EnvFrom: []core.EnvFromSource{{Prefix: "REMOTE_", SecretRef: &core.SecretEnvSource{
					LocalObjectReference: core.LocalObjectReference{Name: "s3-main"},
				}}}})
				useTemplate(chi, warm, "env-from")
			},
			unverifiable: "the clickhouse-backup container of pod template env-from takes environment variables from envFrom",
		},
		"the storage Secret in the pod template's container": {mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
			sidecarTemplate(chi, "env-from", core.Container{EnvFrom: []core.EnvFromSource{{SecretRef: &core.SecretEnvSource{
				LocalObjectReference: core.LocalObjectReference{Name: "s3-main"},
			}}}})
			useTemplate(chi, warm, "env-from")
		}},
		"a configuration file the pod template's container names": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				sidecarTemplate(chi, "config", core.Container{Env: []core.EnvVar{{Name: config.ClickHouseBackupEnvConfig, Value: "/config/backup.yml"}}})
				useTemplate(chi, warm, "config")
			},
			unverifiable: "the clickhouse-backup sidecar reads a configuration file CLICKHOUSE_BACKUP_CONFIG names",
		},
		"a volume where the configuration file is read from": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				sidecarTemplate(chi, "mounted", core.Container{VolumeMounts: []core.VolumeMount{{Name: "config", MountPath: "/etc/clickhouse-backup/"}}})
				chi.Spec.Templates.PodTemplates[len(chi.Spec.Templates.PodTemplates)-1].Spec.Volumes = []core.Volume{{
					Name: "config", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}},
				}}
				useTemplate(chi, warm, "mounted")
			},
			unverifiable: "the clickhouse-backup sidecar mounts a volume at /etc/clickhouse-backup/, where its configuration file may be",
		},
		"a path from a field of the pod": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				sidecarEnv(chi, warm, []core.EnvVar{{Name: "S3_PATH", ValueFrom: &core.EnvVarSource{
					FieldRef: &core.ObjectFieldSelector{FieldPath: "metadata.name"},
				}}})
			},
			unverifiable: "S3_PATH of the clickhouse-backup sidecar takes its value from the pod, which differs in a Job",
		},
		"a path that refers to another variable": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				sidecarEnv(chi, warm, []core.EnvVar{
					{Name: "POD", ValueFrom: &core.EnvVarSource{FieldRef: &core.ObjectFieldSelector{FieldPath: "metadata.name"}}},
					{Name: "S3_PATH", Value: "backup/$(POD)"},
				})
			},
			unverifiable: "S3_PATH of the clickhouse-backup sidecar refers to other environment variables, which resolve otherwise in a Job",
		},
		"envFrom of the container of a sidecar-only pod template": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				chi.Spec.Templates.PodTemplates = append(chi.Spec.Templates.PodTemplates, api.PodTemplate{
					Name: "sidecar-only",
					Spec: core.PodSpec{Containers: []core.Container{{
						Name: config.ClickHouseBackupContainerName,
						EnvFrom: []core.EnvFromSource{{ConfigMapRef: &core.ConfigMapEnvSource{
							LocalObjectReference: core.LocalObjectReference{Name: "paths"},
						}}},
					}}},
				})
				warm.Backup.PodTemplate = types.NewString("sidecar-only")
			},
			unverifiable: "the clickhouse-backup container of pod template sidecar-only takes environment variables from envFrom",
		},
		"a host after the first whose ClickHouse container mounts a configuration file": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				clickHouseTemplate(chi, "config-file", core.VolumeMount{
					Name: "config", MountPath: "/etc/clickhouse-backup/config.yml", SubPath: "config.yml",
				})
				warm.Layout = &api.ChiClusterLayout{Shards: []*api.ChiShard{
					{Name: "0", ReplicasCount: 1},
					{Name: "1", ReplicasCount: 1, Templates: &api.TemplatesList{PodTemplate: "config-file"}},
				}}
			},
			unverifiable: "the clickhouse-backup sidecar mounts a volume at /etc/clickhouse-backup/config.yml, where its configuration file may be",
		},
		"a configuration file mounted on the ClickHouse container": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				clickHouseTemplate(chi, "config-file", core.VolumeMount{
					Name: "config", MountPath: "/etc/clickhouse-backup/config.yml", SubPath: "config.yml",
				})
				useTemplate(chi, warm, "config-file")
			},
			unverifiable: "the clickhouse-backup sidecar mounts a volume at /etc/clickhouse-backup/config.yml, where its configuration file may be",
		},
		"a volume mounted above where the configuration file is read from": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				sidecarTemplate(chi, "etc", core.Container{VolumeMounts: []core.VolumeMount{{Name: "etc", MountPath: "/etc"}}})
				chi.Spec.Templates.PodTemplates[len(chi.Spec.Templates.PodTemplates)-1].Spec.Volumes = []core.Volume{{
					Name: "etc", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}},
				}}
				useTemplate(chi, warm, "etc")
			},
			unverifiable: "the clickhouse-backup sidecar mounts a volume at /etc, where its configuration file may be",
		},
		"a configuration file the command names by the flag and the next argument": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				commandTemplate(chi, "config-flag", []string{"clickhouse-backup", "server", "--config", "/config/backup.yml"}, nil)
				useTemplate(chi, warm, "config-flag")
			},
			unverifiable: "the clickhouse-backup container of pod template config-flag runs a command that may name a configuration file",
		},
		"a configuration file the arguments name by the alias": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				commandTemplate(chi, "config-alias", []string{config.ClickHouseBackupCommand, config.ClickHouseBackupCommandServer},
					[]string{"-c", "/config/backup.yml"})
				useTemplate(chi, warm, "config-alias")
			},
			unverifiable: "the clickhouse-backup container of pod template config-alias runs a command that may name a configuration file",
		},
		"a configuration file the command names by the flag with its value": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				commandTemplate(chi, "config-value", []string{"clickhouse-backup", "--config=/config/backup.yml", "server"}, nil)
				useTemplate(chi, warm, "config-value")
			},
			unverifiable: "the clickhouse-backup container of pod template config-value runs a command that may name a configuration file",
		},
		"a configuration file the command names by the alias with its value": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				commandTemplate(chi, "alias-value", []string{"clickhouse-backup", "server", "-c=/config/backup.yml"}, nil)
				useTemplate(chi, warm, "alias-value")
			},
			unverifiable: "the clickhouse-backup container of pod template alias-value runs a command that may name a configuration file",
		},
		"a configuration file a shell command line names": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				commandTemplate(chi, "shell", []string{"/entrypoint.sh", "BACKUP_ARGS=--config='/config/backup.yml' server"}, nil)
				useTemplate(chi, warm, "shell")
			},
			unverifiable: "the clickhouse-backup container of pod template shell runs a command that may name a configuration file",
		},
		"a configuration file a quoted word of a shell command line names": {
			mutate: func(chi *api.ClickHouseInstallation, warm *api.Cluster) {
				commandTemplate(chi, "quoted", []string{"/entrypoint.sh", "exec clickhouse-backup '-config' /config/backup.yml server"}, nil)
				useTemplate(chi, warm, "quoted")
			},
			unverifiable: "the clickhouse-backup container of pod template quoted runs a command that may name a configuration file",
		},
	} {
		t.Run(name, func(t *testing.T) {
			inMemory, stored := sidecarSettingsInstallation(t, tc.mutate)
			kube := kubeObjects{secrets: map[string]map[string]string{
				managers.NewNameManager(managers.NameManagerTypeClickHouse).Name(interfaces.NameBackupAutoSecret, inMemory): {
					config.ClickHouseBackupSecretKeyAPIUsername: "api", config.ClickHouseBackupSecretKeyAPIPassword: "secret",
					config.ClickHouseBackupSecretKeyClickHouseUsername: config.ClickHouseBackupUsername,
					config.ClickHouseBackupSecretKeyClickHousePassword: "secret",
				},
				"s3-main":   {"REMOTE_STORAGE": "s3", "S3_BUCKET": "main", "S3_PATH": "main/{cluster}/{shard}", "S3_ACCESS_KEY": "key"},
				"s3-cold":   {"REMOTE_STORAGE": "s3", "S3_BUCKET": "cold", "S3_ENDPOINT": "", "S3_ACCESS_KEY": "key"},
				"bucket":    {"name": "hot"},
				"paths":     {"S3_PATH": "from-env-from/{shard}"},
				"own-paths": {"path": "own/{cluster}/{shard}"},
			}, configMaps: map[string]map[string]string{
				"paths": {"path": "config-map/{cluster}/{shard}"},
			}}
			topology, err := NewTopology(stored, "")
			require.NoError(t, err)
			require.Equal(t, []string{"hot", "cold", "warm"}, topology.ClusterNames())
			target, err := topology.ForCluster("hot")
			require.NoError(t, err)
			job := BuildBackupJob(testBackup("b1"), stored, target)
			jobEnv := kube.resolve(t, &job.Spec.Template.Spec.Containers[0], job.Name)

			for _, sidecar := range runSidecars(t, inMemory, kube) {
				index := slices.Index(topology.ClusterNames(), sidecar.cluster)
				require.Equal(t, sidecar.cluster, jobEnv[pathClusterEnv(index, pathEnvCluster)])
				unverifiable, marked := jobEnv[pathClusterEnv(index, pathEnvUnverifiable)]
				if sidecar.cluster == "warm" && (tc.unverifiable != "") {
					require.True(t, marked, "%s: the Job cannot know the settings", sidecar.service)
					require.Equal(t, tc.unverifiable, unverifiable)
					for _, key := range PathCheckKeys {
						require.NotContains(t, jobEnv, pathClusterEnv(index, key), "no setting of a cluster the Job cannot know")
					}
					continue
				}
				require.False(t, marked, "%s: %s", sidecar.service, unverifiable)
				for _, key := range PathCheckKeys {
					jobValue, jobSet := jobSetting(jobEnv, index, key)
					sidecarValue, sidecarSet := sidecar.env[key]
					require.Equal(t, sidecarSet, jobSet, "%s: %s set", sidecar.service, key)
					require.Equal(t, sidecarValue, jobValue, "%s: %s", sidecar.service, key)
				}
			}
		})
	}
}

// The restore Job of a cluster knows whether the cluster can run ON CLUSTER DDL, which needs ZooKeeper,
// and the smallest max_table_size_to_drop of its hosts, which bounds what a cluster-wide drop may drop:
// ClickHouse's default where nothing sets it, none for 0. A host set at the installation level and at
// its own is taken by the smaller limit, as ClickHouse merges the two in an order of its own.
func TestRestoreJobKnowsWhatDropsOnTheClusterNeed(t *testing.T) {
	limit := func(value string) *api.Settings {
		return api.NewSettings().Set(settingMaxTableSizeToDrop, api.NewSettingScalar(value))
	}
	keeper := &api.ZookeeperConfig{Nodes: api.ZookeeperNodes{{Host: "keeper", Port: types.NewInt32(2181)}}}
	for name, tc := range map[string]struct {
		mutate     func(chi *api.ClickHouseInstallation, cluster *api.Cluster)
		replicated bool
		limit      uint64
	}{
		"nothing set": {limit: clickHouseDefaultMaxTableSizeToDrop},
		"ZooKeeper of the installation": {
			mutate:     func(chi *api.ClickHouseInstallation, cluster *api.Cluster) { chi.Spec.Configuration.Zookeeper = keeper },
			replicated: true, limit: clickHouseDefaultMaxTableSizeToDrop,
		},
		"ZooKeeper of the cluster": {
			mutate:     func(chi *api.ClickHouseInstallation, cluster *api.Cluster) { cluster.Zookeeper = keeper },
			replicated: true, limit: clickHouseDefaultMaxTableSizeToDrop,
		},
		"a limit of the installation": {
			mutate: func(chi *api.ClickHouseInstallation, cluster *api.Cluster) {
				chi.Spec.Configuration.Settings = limit("1000")
			},
			limit: 1000,
		},
		"no limit of the installation": {
			mutate: func(chi *api.ClickHouseInstallation, cluster *api.Cluster) {
				chi.Spec.Configuration.Settings = limit("0")
			},
		},
		"a limit of the cluster": {
			mutate: func(chi *api.ClickHouseInstallation, cluster *api.Cluster) { cluster.Settings = limit("2000") },
			limit:  2000,
		},
		"no limit of the cluster under a limit of the installation": {
			mutate: func(chi *api.ClickHouseInstallation, cluster *api.Cluster) {
				chi.Spec.Configuration.Settings = limit("1000")
				cluster.Settings = limit("0")
			},
			limit: 1000,
		},
		"a smaller limit of one shard": {
			mutate: func(chi *api.ClickHouseInstallation, cluster *api.Cluster) {
				cluster.Settings = limit("0")
				cluster.Layout = &api.ChiClusterLayout{Shards: []*api.ChiShard{
					{Name: "0", ReplicasCount: 1},
					{Name: "1", ReplicasCount: 1, Settings: limit("3000")},
				}}
			},
			limit: 3000,
		},
		"a limit that is no number of bytes": {
			mutate: func(chi *api.ClickHouseInstallation, cluster *api.Cluster) { cluster.Settings = limit("lots") },
			limit:  clickHouseDefaultMaxTableSizeToDrop,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cluster := layoutCluster("main", 2, 1, enabledBackup())
			other := layoutCluster("other", 1, 1, enabledBackup())
			other.Zookeeper = keeper
			other.Settings = limit("1")
			chi := newCHI("demo", cluster, other)
			chi.Spec.Configuration.Settings = nil
			if tc.mutate != nil {
				tc.mutate(chi, cluster)
			}
			_, stored := normalize(t, chi)
			topology, err := NewTopology(stored, "main")
			require.NoError(t, err)
			values := env(container(t, BuildRestoreJob(testRestore("r1"), stored, topology).Spec.Template.Spec))
			require.Equal(t, strconv.FormatBool(tc.replicated), values[envReplicated])
			require.Equal(t, strconv.FormatUint(tc.limit, 10), values[envMaxTableSizeToDrop])
		})
	}
}
