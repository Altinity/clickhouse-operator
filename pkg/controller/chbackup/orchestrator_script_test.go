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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// repositoryScriptDir is the directory of the orchestrator script in the repository, which the controllers of the
// tests read it from, as the operator reads it from its configuration.
const repositoryScriptDir = "../../../config/backup"

func init() {
	orchestratorScriptDir = func() string { return repositoryScriptDir }
}

// withScriptDir makes the controllers read the orchestrator script from dir for the duration of the test.
func withScriptDir(t *testing.T, dir string) {
	t.Helper()
	orchestratorScriptDir = func() string { return dir }
	t.Cleanup(func() { orchestratorScriptDir = func() string { return repositoryScriptDir } })
}

// testOrchestrator is how the Jobs the tests build themselves run the orchestrator script.
func testOrchestrator() model.Orchestrator {
	return model.Orchestrator{
		ConfigMap:          model.OrchestratorConfigMapName(testCHI, "main"),
		ServiceAccountName: OrchestratorServiceAccountName(),
		Transport:          api.BackupModeSidecar,
	}
}

func repositoryScript(t *testing.T) string {
	t.Helper()
	script, err := model.ReadOrchestratorScript(repositoryScriptDir)
	require.NoError(t, err)
	return script
}

// The operator copies the script of its configuration to a ConfigMap of the installation, and keeps it up to date.
func TestOrchestratorKeepsTheConfigMapOfTheScript(t *testing.T) {
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	c := newClient(t, nil)
	r := newReconciler(c, c, c.Scheme(), newFakeCR(chi), api.ClickHouseBackupCRDResourceKind)

	orchestrator, missing, err := r.orchestrator(context.Background(), chi, "main", "")
	require.NoError(t, err)
	require.Nil(t, missing)
	script := repositoryScript(t)
	require.Equal(t, model.OrchestratorConfigMapName(testCHI, "main"), orchestrator.ConfigMap)
	require.Equal(t, model.ScriptSHA256(script), orchestrator.ScriptSHA256)
	require.Equal(t, OrchestratorServiceAccountName(), orchestrator.ServiceAccountName)
	require.Equal(t, api.BackupModeSidecar, orchestrator.Transport)

	configMap := &core.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: orchestrator.ConfigMap}, configMap))
	require.Equal(t, script, configMap.Data[model.OrchestratorScriptFile])
	require.True(t, meta.IsControlledBy(configMap, chi))

	// A new script of the configuration reaches the ConfigMap
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, model.OrchestratorScriptFile), []byte("echo new\n"), 0o644))
	withScriptDir(t, dir)
	orchestrator, missing, err = r.orchestrator(context.Background(), chi, "main", "")
	require.NoError(t, err)
	require.Nil(t, missing)
	require.Equal(t, model.ScriptSHA256("echo new\n"), orchestrator.ScriptSHA256)
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: orchestrator.ConfigMap}, configMap))
	require.Equal(t, "echo new\n", configMap.Data[model.OrchestratorScriptFile])
}

// Without a script there is nothing to run: the operation is aborted, naming the file.
func TestOrchestratorWithoutScript(t *testing.T) {
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	c := newClient(t, nil)
	r := newReconciler(c, c, c.Scheme(), newFakeCR(chi), api.ClickHouseBackupCRDResourceKind)
	dir := t.TempDir()
	withScriptDir(t, dir)

	_, missing, err := r.orchestrator(context.Background(), chi, "main", "")
	require.NoError(t, err)
	require.NotNil(t, missing)
	require.Equal(t, reasonOrchestratorScriptMissing, missing.reason)
	require.Contains(t, missing.message, filepath.Join(dir, model.OrchestratorScriptFile))
	require.True(t, missing.refusesOneOff())
}

// A ConfigMap of that name the installation does not control is never overwritten.
func TestOrchestratorRefusesAForeignConfigMap(t *testing.T) {
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	foreign := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{Name: model.OrchestratorConfigMapName(testCHI, "main"), Namespace: testNamespace},
		Data:       map[string]string{"other": "data"},
	}
	c := newClient(t, nil, foreign)
	r := newReconciler(c, c, c.Scheme(), newFakeCR(chi), api.ClickHouseBackupCRDResourceKind)

	_, missing, err := r.orchestrator(context.Background(), chi, "main", "")
	require.NoError(t, err)
	require.NotNil(t, missing)
	require.Equal(t, reasonOrchestratorScriptMissing, missing.reason)
	kept := &core.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(foreign), kept))
	require.Equal(t, foreign.Data, kept.Data)
}

// An operation that names a ConfigMap of its own runs its script, which the operator never writes.
func TestOrchestratorTakesTheConfigMapOfTheOperation(t *testing.T) {
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	c := newClient(t, nil)
	r := newReconciler(c, c, c.Scheme(), newFakeCR(chi), api.ClickHouseBackupCRDResourceKind)

	_, missing, err := r.orchestrator(context.Background(), chi, "main", "my-script")
	require.NoError(t, err)
	require.NotNil(t, missing, "the ConfigMap the operation names does not exist")
	require.Contains(t, missing.message, "my-script")

	mine := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{Name: "my-script", Namespace: testNamespace},
		Data:       map[string]string{api.BackupOrchestratorScriptKey: "echo mine"},
	}
	require.NoError(t, c.Create(context.Background(), mine))
	orchestrator, missing, err := r.orchestrator(context.Background(), chi, "main", "my-script")
	require.NoError(t, err)
	require.Nil(t, missing)
	require.Equal(t, "my-script", orchestrator.ConfigMap)
	require.Equal(t, model.ScriptSHA256("echo mine"), orchestrator.ScriptSHA256)
	err = c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: model.OrchestratorConfigMapName(testCHI, "main")}, &core.ConfigMap{})
	require.Error(t, err, "no operator ConfigMap for an operation that names its own")
}

// The pod transport drives the backup Pod of every shard.
func TestOrchestratorOfThePodTransport(t *testing.T) {
	cluster := backupCluster("main", 2, true)
	cluster.Backup.Mode = types.NewString(api.BackupModePod)
	chi := installation(t, api.StatusCompleted, cluster)
	c := newClient(t, nil)
	r := newReconciler(c, c, c.Scheme(), newFakeCR(chi), api.ClickHouseBackupCRDResourceKind)

	orchestrator, missing, err := r.orchestrator(context.Background(), chi, "main", "")
	require.NoError(t, err)
	require.Nil(t, missing)
	require.True(t, orchestrator.IsPodTransport())
	require.Equal(t, model.ShardPodHosts(chi.GetAncestorT(), "main"), orchestrator.ShardPodHosts)
	require.Len(t, orchestrator.ShardPodHosts, 2)
}

// A cluster whose backup.orchestrator is python runs the Python script of the configuration, which the operator keeps
// in the ConfigMap of the cluster under its own key; switched back to bash, the ConfigMap holds the bash script alone.
func TestOrchestratorOfThePythonOrchestrator(t *testing.T) {
	cluster := backupCluster("main", 1, true)
	cluster.Backup.Orchestrator = types.NewString(api.BackupOrchestratorPython)
	chi := installation(t, api.StatusCompleted, cluster)
	c := newClient(t, nil)
	r := newReconciler(c, c, c.Scheme(), newFakeCR(chi), api.ClickHouseBackupCRDResourceKind)

	orchestrator, missing, err := r.orchestrator(context.Background(), chi, "main", "")
	require.NoError(t, err)
	require.Nil(t, missing)
	require.True(t, orchestrator.IsPython())
	script, err := model.ReadOrchestratorScriptOf(repositoryScriptDir, api.BackupOrchestratorPython)
	require.NoError(t, err)
	require.Equal(t, model.ScriptSHA256(script), orchestrator.ScriptSHA256)
	configMap := &core.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: orchestrator.ConfigMap}, configMap))
	require.Equal(t, map[string]string{api.BackupOrchestratorPythonScriptKey: script}, configMap.Data)

	bash := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	orchestrator, missing, err = r.orchestrator(context.Background(), bash, "main", "")
	require.NoError(t, err)
	require.Nil(t, missing)
	require.False(t, orchestrator.IsPython())
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: orchestrator.ConfigMap}, configMap))
	require.Equal(t, map[string]string{api.BackupOrchestratorScriptKey: repositoryScript(t)}, configMap.Data)
}

// The ConfigMap an operation names has to hold the script of the orchestrator of the cluster: one that holds the
// script of the other one aborts the operation, saying which.
func TestOrchestratorRefusesTheScriptOfTheOtherOrchestrator(t *testing.T) {
	cluster := backupCluster("main", 1, true)
	cluster.Backup.Orchestrator = types.NewString(api.BackupOrchestratorPython)
	chi := installation(t, api.StatusCompleted, cluster)
	bashScript := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{Name: "my-script", Namespace: testNamespace},
		Data:       map[string]string{api.BackupOrchestratorScriptKey: "echo mine"},
	}
	c := newClient(t, nil, bashScript)
	r := newReconciler(c, c, c.Scheme(), newFakeCR(chi), api.ClickHouseBackupCRDResourceKind)

	_, missing, err := r.orchestrator(context.Background(), chi, "main", "my-script")
	require.NoError(t, err)
	require.NotNil(t, missing)
	require.Equal(t, reasonOrchestratorScriptMismatch, missing.reason)
	require.Contains(t, missing.message, "holds the script backup-orchestrator.sh, while backup.orchestrator of cluster main is python")
	require.True(t, missing.refusesOneOff())

	bashScript.Data = map[string]string{api.BackupOrchestratorScriptKey: "echo mine", api.BackupOrchestratorPythonScriptKey: "print()"}
	require.NoError(t, c.Update(context.Background(), bashScript))
	orchestrator, missing, err := r.orchestrator(context.Background(), chi, "main", "my-script")
	require.NoError(t, err)
	require.Nil(t, missing, "a ConfigMap of both scripts serves either orchestrator")
	require.Equal(t, model.ScriptSHA256("print()"), orchestrator.ScriptSHA256)
}
