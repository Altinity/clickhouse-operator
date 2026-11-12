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
	"crypto/sha256"
	"encoding/hex"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	clickhouse_altinity_com "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

const (
	// OrchestratorMountPath is where the Job container finds the orchestrator script, the OrchestratorScriptFile of
	// the ConfigMap mounted there.
	OrchestratorMountPath = "/etc/clickhouse-backup-orchestrator"
	// orchestratorVolumeName is the volume of that ConfigMap in the Job pod.
	orchestratorVolumeName = "backup-orchestrator"
	// orchestratorConfigMapSuffix ends the name of the ConfigMap the operator keeps the script of a cluster in.
	orchestratorConfigMapSuffix = "backup-orchestrator"
	// backupComponentOrchestrator is the LabelBackupComponent of the ConfigMap of the orchestrator script.
	backupComponentOrchestrator = "orchestrator"
)

// AnnotationOrchestratorScriptSHA256 carries, on the pod template of a Job or CronJob, the SHA-256 of the orchestrator
// script it runs: a change of the script changes the template, so a CronJob is rebuilt and its next run takes it.
const AnnotationOrchestratorScriptSHA256 = clickhouse_altinity_com.APIGroupName + "/backup-orchestrator-sha256"

// Orchestrator is what a Job pod needs to run the orchestrator script, besides what the topology tells.
type Orchestrator struct {
	// ConfigMap names the ConfigMap, in the namespace of the Job, whose key OrchestratorScriptFile is the script:
	// the one the operator keeps, OrchestratorConfigMapName, or the one the spec.orchestratorConfigMapRef of the operation names.
	ConfigMap string
	// ScriptSHA256 is the hash of the script, ScriptSHA256, "" when unknown.
	ScriptSHA256 string
	// ServiceAccountName is the ServiceAccount the Job pod runs as, which posts the Events of the script.
	ServiceAccountName string
	// Transport is BackupModeSidecar or BackupModePod, the mode of the targeted cluster.
	Transport string
	// ShardPodHosts are the DNS names of the backup Pods of the shards of the cluster, for the pod transport.
	ShardPodHosts []string
	// RestoreInProgress tells that a restore of the cluster is not finished: a scheduled run skips retention.
	RestoreInProgress bool
}

// IsPodTransport reports whether the Job reaches clickhouse-backup through the backup Pods of the shards.
func (o Orchestrator) IsPodTransport() bool {
	return o.Transport == api.BackupModePod
}

// ScriptSHA256 returns the hex SHA-256 of an orchestrator script.
func ScriptSHA256(script string) string {
	sum := sha256.Sum256([]byte(script))
	return hex.EncodeToString(sum[:])
}

// OrchestratorConfigMapName returns the name of the ConfigMap the operator keeps the orchestrator script of a cluster
// of an installation in: <chi>-<cluster>-backup-orchestrator.
func OrchestratorConfigMapName(chiName, cluster string) string {
	return boundedName(joinName(chiName, cluster), orchestratorConfigMapSuffix, validation.DNS1123SubdomainMaxLength)
}

// BuildOrchestratorConfigMap builds the ConfigMap the operator keeps the orchestrator script of a cluster of an
// installation in, owned by the installation.
func BuildOrchestratorConfigMap(chi *api.ClickHouseInstallation, cluster, script string) *core.ConfigMap {
	labels := map[string]string{
		LabelBackupCHI:       LabelValue(chi.GetName()),
		LabelBackupCluster:   LabelValue(cluster),
		LabelBackupComponent: backupComponentOrchestrator,
	}
	return &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:      OrchestratorConfigMapName(chi.GetName(), cluster),
			Namespace: chi.GetNamespace(),
			Labels:    labels,
			Annotations: map[string]string{
				AnnotationOrchestratorScriptSHA256: ScriptSHA256(script),
			},
			OwnerReferences: []meta.OwnerReference{ShardPodOwner(chi)},
		},
		Data: map[string]string{OrchestratorScriptFile: script},
	}
}

// ClusterBackupOf returns the backup settings of a cluster of a normalized installation, nil when it has no such
// cluster.
func ClusterBackupOf(normalized *api.ClickHouseInstallation, cluster string) *api.ClusterBackup {
	if normalized == nil {
		return nil
	}
	for _, c := range normalized.GetSpecT().GetConfiguration().GetClusters() {
		if c.GetName() == cluster {
			return c.GetBackup()
		}
	}
	return nil
}

// orchestratorVolume returns the volume of the ConfigMap of the script.
func orchestratorVolume(configMap string) core.Volume {
	return core.Volume{
		Name: orchestratorVolumeName,
		VolumeSource: core.VolumeSource{
			ConfigMap: &core.ConfigMapVolumeSource{
				LocalObjectReference: core.LocalObjectReference{Name: configMap},
				Items:                []core.KeyToPath{{Key: OrchestratorScriptFile, Path: OrchestratorScriptFile}},
			},
		},
	}
}

// orchestratorCommand runs the script from the ConfigMap. The script text is static: every value it acts on reaches
// it through the environment, so no user input is ever parsed as shell code.
func orchestratorCommand() []string {
	return []string{"bash", OrchestratorMountPath + "/" + OrchestratorScriptFile}
}
