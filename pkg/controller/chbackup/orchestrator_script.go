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
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"strings"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeTypes "k8s.io/apimachinery/pkg/types"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// orchestratorScriptDir returns the directory the operator reads the orchestrator script from: backup.path of the
// operator configuration, "" when that folder does not exist. A variable for the tests.
var orchestratorScriptDir = func(orchestrator string) string {
	if orchestrator == api.BackupOrchestratorPython {
		return chop.Config().Backup.PythonPath
	}
	return chop.Config().Backup.Path
}

// orchestratorScriptMissing explains an operation, or a schedule, that cannot run as there is no orchestrator script
// to run: it is aborted, as only a change of the operator configuration, or of the ConfigMap the cluster names, can
// bring one.
func orchestratorScriptMissing(message string) *pending {
	return &pending{reason: reasonOrchestratorScriptMissing, message: message, requeue: targetRequeueInterval}
}

// orchestratorScriptMismatch explains an operation, or a schedule, whose cluster runs one orchestrator while the
// ConfigMap its spec.orchestratorConfigMapRef names holds the script of the other: it is aborted, as only a change of
// either can settle it.
func orchestratorScriptMismatch(message string) *pending {
	return &pending{reason: reasonOrchestratorScriptMismatch, message: message, requeue: targetRequeueInterval}
}

// orchestrator prepares what the Jobs of a cluster of an installation need to run the orchestrator script: the
// ConfigMap that holds it, configMap, the one the spec.orchestratorConfigMapRef of the operation or the schedule names,
// in its namespace, the namespace of the installation, or, when it names none, the one the operator keeps from its
// configuration; the ServiceAccount they run as and the transport of the cluster. It returns why no Job can run when
// there is no script.
func (r *reconciler) orchestrator(
	ctx context.Context,
	chi *api.ClickHouseInstallation,
	cluster string,
	configMap string,
) (model.Orchestrator, *pending, error) {
	normalized := chi.GetAncestorT()
	backup := model.ClusterBackupOf(normalized, cluster)
	orchestrator := model.Orchestrator{Transport: backup.GetMode(), Language: backup.GetOrchestrator()}
	key := api.BackupOrchestratorScriptKeyOf(orchestrator.Language)
	if orchestrator.IsPodTransport() {
		orchestrator.ShardPodHosts = model.ShardPodHosts(normalized, cluster)
	}

	if name := configMap; name != "" {
		configMap := &core.ConfigMap{}
		err := r.apiReader.Get(ctx, kubeTypes.NamespacedName{Namespace: chi.GetNamespace(), Name: name}, configMap)
		switch {
		case apiErrors.IsNotFound(err):
			return orchestrator, orchestratorScriptMissing(fmt.Sprintf("ConfigMap %s, which spec.orchestratorConfigMapRef names, "+
				"not found", name)), nil
		case err != nil:
			return orchestrator, nil, err
		}
		script := configMap.Data[key]
		if strings.TrimSpace(script) == "" {
			other := api.BackupOrchestratorScriptKeyOf(api.BackupOrchestratorBash)
			if other == key {
				other = api.BackupOrchestratorScriptKeyOf(api.BackupOrchestratorPython)
			}
			if strings.TrimSpace(configMap.Data[other]) != "" {
				return orchestrator, orchestratorScriptMismatch(fmt.Sprintf("ConfigMap %s, which spec.orchestratorConfigMapRef "+
					"names, holds the script %s, while backup.orchestrator of cluster %s is %s, which runs %s: name a ConfigMap "+
					"with the key %s, or set backup.orchestrator to the orchestrator of the script", name, other, cluster,
					orchestrator.Language, key, key)), nil
			}
			return orchestrator, orchestratorScriptMissing(fmt.Sprintf("ConfigMap %s, which spec.orchestratorConfigMapRef names, "+
				"has no key %s with the orchestrator script", name, key)), nil
		}
		orchestrator.ConfigMap = name
		orchestrator.ScriptSHA256 = model.ScriptSHA256(script)
	} else {
		dir := orchestratorScriptDir(orchestrator.Language)
		if dir == "" {
			folder := "backup.path"
			if orchestrator.IsPython() {
				folder = "backup.pythonPath"
			}
			return orchestrator, orchestratorScriptMissing(fmt.Sprintf("no backup orchestrator script %s: the folder %s "+
				"of the operator configuration does not exist", key, folder)), nil
		}
		script, err := model.ReadOrchestratorScriptOf(dir, orchestrator.Language)
		if err != nil {
			return orchestrator, orchestratorScriptMissing(fmt.Sprintf("no backup orchestrator script %s: %v",
				filepath.Join(dir, key), err)), nil
		}
		desired := model.BuildOrchestratorConfigMap(chi, cluster, orchestrator.Language, script)
		if wait, err := r.applyOrchestratorConfigMap(ctx, chi, desired); (wait != nil) || (err != nil) {
			return orchestrator, wait, err
		}
		orchestrator.ConfigMap = desired.GetName()
		orchestrator.ScriptSHA256 = model.ScriptSHA256(script)
	}

	serviceAccount, err := r.EnsureOrchestratorServiceAccount(ctx, chi.GetNamespace())
	if err != nil {
		return orchestrator, nil, fmt.Errorf("unable to ensure the ServiceAccount of the backup orchestrator: %w", err)
	}
	orchestrator.ServiceAccountName = serviceAccount
	return orchestrator, nil, nil
}

// applyOrchestratorConfigMap creates the ConfigMap of the orchestrator script of a cluster, or updates the script it
// holds. ConfigMaps are read past the cache, which holds none. A ConfigMap of that name the installation does not
// control is never taken over.
func (r *reconciler) applyOrchestratorConfigMap(ctx context.Context, chi *api.ClickHouseInstallation, desired *core.ConfigMap) (*pending, error) {
	existing := &core.ConfigMap{}
	err := r.apiReader.Get(ctx, kubeTypes.NamespacedName{Namespace: desired.GetNamespace(), Name: desired.GetName()}, existing)
	switch {
	case apiErrors.IsNotFound(err):
		if err := r.Create(ctx, desired); (err != nil) && !apiErrors.IsAlreadyExists(err) {
			return nil, err
		}
		log.V(1).M(chi).F().Info("created ConfigMap %s of the backup orchestrator script", desired.GetName())
		return nil, nil
	case err != nil:
		return nil, err
	case !meta.IsControlledBy(existing, chi):
		return orchestratorScriptMissing(fmt.Sprintf("the ConfigMap name %s of the backup orchestrator script is taken by a "+
			"ConfigMap ClickHouseInstallation %s does not control: delete or rename it, or name it in "+
			"spec.orchestratorConfigMapRef", desired.GetName(), chi.GetName())), nil
	}
	sha := desired.GetAnnotations()[model.AnnotationOrchestratorScriptSHA256]
	if maps.Equal(existing.Data, desired.Data) && (existing.GetAnnotations()[model.AnnotationOrchestratorScriptSHA256] == sha) {
		return nil, nil
	}
	updated := existing.DeepCopy()
	updated.Data = desired.Data
	meta.SetMetaDataAnnotation(&updated.ObjectMeta, model.AnnotationOrchestratorScriptSHA256, sha)
	for key, value := range desired.GetLabels() {
		meta.SetMetaDataLabel(&updated.ObjectMeta, key, value)
	}
	if err := r.Update(ctx, updated); err != nil {
		return nil, err
	}
	log.V(1).M(chi).F().Info("updated ConfigMap %s of the backup orchestrator script", desired.GetName())
	return nil, nil
}
