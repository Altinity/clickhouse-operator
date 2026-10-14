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
	"fmt"
	"path"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	chiCreator "github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

// envVarReferencePrefix starts a reference to another env var in a value, which kubelet expands from
// the env vars of the container it starts.
const envVarReferencePrefix = "$("

// The flag clickhouse-backup takes its configuration file from, which it prefers to the file
// config.ClickHouseBackupEnvConfig names, by its name and its alias. Its command line parser takes either with one
// dash or two, and the file after "=" or as the next argument.
const (
	configFlagName      = "config"
	configFlagAlias     = "c"
	commandFlagDash     = "-"
	commandFlagValueSep = "="
)

// commandWordQuotes are the quotes a shell takes off a word of a command line it runs.
const commandWordQuotes = `"'`

// sidecarSettings is how the clickhouse-backup sidecar of a host takes the settings the storage path
// check reads. The operator never reads a Secret: it tells the Job where every setting comes from, and
// kubelet resolves it for the Job as it does for the sidecar.
type sidecarSettings struct {
	// defaultsPaths tells whether the sidecar runs the operator's own command, which defaults the storage
	// paths its environment leaves unset
	defaultsPaths bool
	// sources holds, by key of PathCheckKeys, the env vars that set the key in the sidecar, in the order
	// kubelet applies them, each made optional so that it never keeps the Job from starting. A key nothing
	// sets has none.
	sources map[string][]core.EnvVar
	// unverifiable tells why the Job cannot know a setting of the sidecar, "" when it can
	unverifiable string
}

// newSidecarSettings tells how the sidecar of a host takes the settings the storage path check reads, from
// the sidecar as the creator builds it. What the Job cannot follow makes the settings unverifiable: env vars
// from an envFrom source other than the storage Secret, a configuration file named other than by
// that Secret, and a value that resolves otherwise in a pod of the Job. A file that Secret names reaches the
// Job as a setting, which leaves the cluster out just as well when set.
func newSidecarSettings(host *api.Host, containers interfaces.IContainerManager) sidecarSettings {
	sidecar, _ := containers.NewBackupContainer(host)
	settings := sidecarSettings{defaultsPaths: chiCreator.IsBackupSidecarCommand(&sidecar), sources: map[string][]core.EnvVar{}}
	storageSecret := host.GetCluster().GetBackup().GetStorageSecretName()
	mounts := slices.Clone(sidecar.VolumeMounts)
	// What the operator does not set itself comes from the clickhouse-backup container of the pod template the
	// cluster's backup names
	if template, ok := host.GetBackupPodTemplate(); ok {
		if slices.ContainsFunc(sidecar.EnvFrom, func(source core.EnvFromSource) bool {
			return (source.SecretRef == nil) || (source.SecretRef.Name != storageSecret) || (source.Prefix != "")
		}) {
			return settings.cannotVerify(fmt.Sprintf("the %s container of pod template %s takes environment variables from envFrom",
				config.ClickHouseBackupContainerName, template.Name))
		}
		if !settings.defaultsPaths && (namesConfigFile(sidecar.Command) || namesConfigFile(sidecar.Args)) {
			return settings.cannotVerify(fmt.Sprintf("the %s container of pod template %s runs a command that may name a configuration "+
				"file", config.ClickHouseBackupContainerName, template.Name))
		}
	}
	// The sidecar mounts every volume of the ClickHouse container as well
	if template, ok := host.GetPodTemplate(); ok {
		if clickHouse, ok := chiCreator.PodSpecContainerGetClickHouse(&template.Spec); ok {
			mounts = append(mounts, clickHouse.VolumeMounts...)
		}
	}
	for _, mount := range mounts {
		if pathsOverlap(mount.MountPath, config.DirPathClickHouseBackupConfig) {
			return settings.cannotVerify(fmt.Sprintf("the %s sidecar mounts a volume at %s, where its configuration file may be",
				config.ClickHouseBackupContainerName, mount.MountPath))
		}
	}
	if lastEnvVar(sidecar.Env, config.ClickHouseBackupEnvConfig) != nil {
		return settings.cannotVerify(fmt.Sprintf("the %s sidecar reads a configuration file %s names",
			config.ClickHouseBackupContainerName, config.ClickHouseBackupEnvConfig))
	}
	for _, key := range PathCheckKeys {
		sources, unverifiable := sidecarSettingSources(storageSecret, sidecar.Env, key)
		if unverifiable != "" {
			return settings.cannotVerify(unverifiable)
		}
		if len(sources) > 0 {
			settings.sources[key] = sources
		}
	}
	return settings
}

// cannotVerify returns the settings marked unverifiable for the reason given, as one line.
func (s sidecarSettings) cannotVerify(reason string) sidecarSettings {
	s.unverifiable = strings.Join(strings.Fields(reason), " ")
	s.sources = nil
	return s
}

// equal reports whether two hosts take the settings alike.
func (s sidecarSettings) equal(other sidecarSettings) bool {
	return (s.defaultsPaths == other.defaultsPaths) && (s.unverifiable == other.unverifiable) &&
		apiEquality.Semantic.DeepEqual(s.sources, other.sources)
}

// sidecarSettingSources returns the env vars that set the key in the sidecar, in the order kubelet applies
// them, or why the Job cannot follow them: the key of the storage Secret, which the sidecar takes
// through envFrom, before every env var of the sidecar. What an entry that always sets the variable comes
// after is overridden, and is left out. An optional source that lacks the key leaves the variable as it
// was, in the sidecar and in the Job alike.
func sidecarSettingSources(storageSecret string, env []core.EnvVar, key string) ([]core.EnvVar, string) {
	var entries []core.EnvVar
	if storageSecret != "" {
		entries = append(entries, k8s.NewEnvVarFromSecret(key, storageSecret, key))
	}
	for i := range env {
		if env[i].Name == key {
			entries = append(entries, env[i])
		}
	}
	start := 0
	for i := range entries {
		if alwaysSets(entries[i]) {
			start = i
		}
	}
	entries = entries[start:]

	var sources []core.EnvVar
	for i := range entries {
		entry := &entries[i]
		switch {
		case entry.ValueFrom == nil:
			if strings.Contains(entry.Value, envVarReferencePrefix) {
				return nil, fmt.Sprintf("%s of the %s sidecar refers to other environment variables, which resolve otherwise in a Job",
					key, config.ClickHouseBackupContainerName)
			}
			sources = append(sources, core.EnvVar{Name: key, Value: entry.Value})
		case (entry.ValueFrom.SecretKeyRef != nil) || (entry.ValueFrom.ConfigMapKeyRef != nil):
			source := entry.ValueFrom.DeepCopy()
			if source.SecretKeyRef != nil {
				source.SecretKeyRef.Optional = ptr.To(true)
			}
			if source.ConfigMapKeyRef != nil {
				source.ConfigMapKeyRef.Optional = ptr.To(true)
			}
			sources = append(sources, core.EnvVar{Name: key, ValueFrom: source})
		default:
			return nil, fmt.Sprintf("%s of the %s sidecar takes its value from the pod, which differs in a Job",
				key, config.ClickHouseBackupContainerName)
		}
	}
	return sources, ""
}

// alwaysSets reports whether an env var sets the variable whatever its source holds: a value, or a source
// that is not optional, without which kubelet does not start the container at all.
func alwaysSets(envVar core.EnvVar) bool {
	switch {
	case envVar.ValueFrom == nil:
		return true
	case envVar.ValueFrom.SecretKeyRef != nil:
		return !ptr.Deref(envVar.ValueFrom.SecretKeyRef.Optional, false)
	case envVar.ValueFrom.ConfigMapKeyRef != nil:
		return !ptr.Deref(envVar.ValueFrom.ConfigMapKeyRef.Optional, false)
	}
	return true
}

// lastEnvVar returns the last env var of the name, which kubelet applies over the others, nil for none.
func lastEnvVar(env []core.EnvVar, name string) *core.EnvVar {
	for i := len(env) - 1; i >= 0; i-- {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
}

// namesConfigFile reports whether a command line may name a configuration file of clickhouse-backup: a word of
// an argument, which a shell may split it into, is the flag that does. Any argument holding the flag with two
// dashes counts as well, wherever a wrapper puts it. A command line that only looks so, as a flag of another
// command does, counts all the same: the settings of the sidecar are then taken as unknown, never wrongly as
// known.
func namesConfigFile(args []string) bool {
	for _, arg := range args {
		if strings.Contains(arg, commandFlagDash+commandFlagDash+configFlagName) {
			return true
		}
		for _, word := range strings.Fields(arg) {
			word = strings.Trim(word, commandWordQuotes)
			if !strings.HasPrefix(word, commandFlagDash) {
				continue
			}
			name, _, _ := strings.Cut(strings.TrimLeft(word, commandFlagDash), commandFlagValueSep)
			if (name == configFlagName) || (name == configFlagAlias) {
				return true
			}
		}
	}
	return false
}

// pathsOverlap reports whether one path is the other or lies below it.
func pathsOverlap(x, y string) bool {
	x, y = path.Clean(x), path.Clean(y)
	within := func(target, dir string) bool {
		return (dir == "/") || (target == dir) || strings.HasPrefix(target, dir+"/")
	}
	return within(x, y) || within(y, x)
}
