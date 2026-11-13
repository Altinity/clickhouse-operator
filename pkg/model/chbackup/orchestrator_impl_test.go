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
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The orchestrators the script tests run: the bash one, and the Python one, which runs the same scenarios against the
// same fake REST API and Kubernetes API, so that both behave alike. The tests run the one CHBACKUP_TEST_ORCHESTRATOR
// names, bash by default; TestPythonOrchestratorParity runs the tests of the bash one again with the Python one.
const (
	orchestratorBash   = "bash"
	orchestratorPython = "python"
	// testedOrchestratorEnv names the orchestrator the script tests run
	testedOrchestratorEnv = "CHBACKUP_TEST_ORCHESTRATOR"
)

var testedOrchestrator = cmp.Or(os.Getenv(testedOrchestratorEnv), orchestratorBash)

// pythonOrchestratorScriptDir returns the directory of the Python orchestrator script in the repository, that of the
// bash one, generated from deploy/builder/templates-config/backup
func pythonOrchestratorScriptDir() string {
	return orchestratorScriptDir()
}

// readScript returns the text of a script file.
func readScript(path string) (string, error) {
	raw, err := os.ReadFile(path)
	return string(raw), err
}

// bashOnly skips a test of the text or the functions of the bash script when the tests run the Python orchestrator.
func bashOnly(t *testing.T) {
	t.Helper()
	if testedOrchestrator != orchestratorBash {
		t.Skip("a test of the bash orchestrator alone")
	}
}

// pythonVersionOK reports whether python3 is 3.10 or newer, which the Python orchestrator needs.
func pythonVersionOK() bool {
	return exec.Command("python3", "-c", "import sys; sys.exit(sys.version_info < (3, 10))").Run() == nil
}

// requireOrchestrator skips a test when the tools the tested orchestrator needs are missing.
func requireOrchestrator(t *testing.T) {
	t.Helper()
	if testedOrchestrator == orchestratorPython {
		if !pythonVersionOK() {
			t.Skip("python3 3.10 or newer is required to run the Python orchestrator")
		}
		return
	}
	for _, tool := range []string{"bash", "curl", "base64", "sort", "awk", "tr", "date"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is required to run the Job script: %v", tool, err)
		}
	}
}

// scriptCommand returns the command that runs script, a variant of the tested orchestrator, from a file, as the
// Job runs it: as an argument it would exceed the length Linux allows one, MAX_ARG_STRLEN.
func scriptCommand(ctx context.Context, t *testing.T, script string) *exec.Cmd {
	t.Helper()
	name, interpreter := OrchestratorScriptFile, "bash"
	if testedOrchestrator == orchestratorPython {
		name, interpreter = OrchestratorPythonScriptFile, "python3"
	}
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(script), 0o600))
	return exec.CommandContext(ctx, interpreter, path)
}

// The Python orchestrator behaves as the bash one: every script test of the bash one that runs it against the fake APIs
// runs again with the Python one, in a run of this test binary of its own.
func TestPythonOrchestratorParity(t *testing.T) {
	if testedOrchestrator != orchestratorBash {
		t.Skip("the parity run itself")
	}
	if !pythonVersionOK() {
		t.Skip("python3 3.10 or newer is required to run the Python orchestrator")
	}
	cmd := exec.Command(os.Args[0], "-test.run", "^TestJobScript", "-test.count=1")
	cmd.Env = append(os.Environ(), testedOrchestratorEnv+"="+orchestratorPython)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "the script tests fail with the Python orchestrator:\n%s", lastLines(string(out), 400))
}

// The Python orchestrator compiles.
func TestPythonOrchestratorCompiles(t *testing.T) {
	if !pythonVersionOK() {
		t.Skip("python3 3.10 or newer is required to run the Python orchestrator")
	}
	out, err := exec.Command("python3", "-m", "py_compile", filepath.Join(pythonOrchestratorScriptDir(), OrchestratorPythonScriptFile)).CombinedOutput()
	require.NoError(t, err, string(out))
}

// lastLines returns the last n lines of text.
func lastLines(text string, n int) string {
	lines := strings.Split(text, "\n")
	if len(lines) > n {
		lines = append([]string{fmt.Sprintf("... %d lines before", len(lines)-n)}, lines[len(lines)-n:]...)
	}
	return strings.Join(lines, "\n")
}

// imported runs snippet, Python code, in the namespace of the Python orchestrator imported, which runs nothing then
// but leaves its functions, with only the given environment on top of PATH, and returns its exit code and output.
func imported(t *testing.T, env map[string]string, snippet string) (int, string) {
	t.Helper()
	if !pythonVersionOK() {
		t.Skip("python3 3.10 or newer is required to run the Python orchestrator")
	}
	path := filepath.Join(t.TempDir(), "backup_orchestrator.py")
	require.NoError(t, os.WriteFile(path, []byte(orchestratorScript), 0o600))
	loader := "import importlib.util, sys\n" +
		"spec = importlib.util.spec_from_file_location('backup_orchestrator', sys.argv[1])\n" +
		"module = importlib.util.module_from_spec(spec)\n" +
		"spec.loader.exec_module(module)\n" +
		"exec(compile(sys.argv[2], 'snippet', 'exec'), vars(module))\n"
	cmd := exec.Command("python3", "-c", loader, path, snippet)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	for name, value := range env {
		cmd.Env = append(cmd.Env, name+"="+value)
	}
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), string(out)
	}
	require.NoError(t, err)
	return 0, string(out)
}

// snippetOutput runs the snippet of the tested orchestrator, bash sourced or Python imported, which has to print the
// same, and requires it to succeed, returning its output.
func snippetOutput(t *testing.T, env map[string]string, bash, python string) string {
	t.Helper()
	if testedOrchestrator == orchestratorPython {
		code, out := imported(t, env, python)
		require.Zero(t, code, out)
		return out
	}
	return sourcedOutput(t, env, bash)
}

// snippet runs the snippet of the tested orchestrator, bash sourced or Python imported, and returns its exit code and
// output.
func snippet(t *testing.T, env map[string]string, bash, python string) (int, string) {
	t.Helper()
	if testedOrchestrator == orchestratorPython {
		return imported(t, env, python)
	}
	return sourced(t, env, bash)
}

// pythonScript returns the Python orchestrator of the repository.
func pythonScript(t *testing.T) string {
	t.Helper()
	script, err := readScript(filepath.Join(pythonOrchestratorScriptDir(), OrchestratorPythonScriptFile))
	require.NoError(t, err)
	return script
}

// The Python orchestrator reads every variable of the environment of a Job the bash one reads, and the path check
// settings of every cluster under the names the operator gives them.
func TestPythonOrchestratorReadsDeclaredEnv(t *testing.T) {
	script := pythonScript(t)
	for _, name := range declaredScriptEnv {
		require.Contains(t, script, "'"+name+"'", name)
	}
	require.Contains(t, script, "PATH_{index}_{name}")
	for _, key := range PathCheckKeys {
		require.Contains(t, script, "'"+key+"'", key)
	}
	for _, name := range []string{pathEnvCluster, pathEnvShards, pathEnvUnverifiable} {
		require.Contains(t, script, "path_setting(index, '"+name+"')", name)
	}
}

// The Python orchestrator checks the values of the environment as the bash one does: the same regexes and bounds.
func TestPythonOrchestratorConstantsMatchTheBashOnes(t *testing.T) {
	bash, err := readScript(filepath.Join(orchestratorScriptDir(), OrchestratorScriptFile))
	require.NoError(t, err)
	python := pythonScript(t)
	names := regexp.MustCompile(`(?m)^([A-Z][A-Z0-9_]*_(RE|MAX_LENGTH|TIMEOUT|SECONDS|MAX|ITEMS|TOLERANCE)|DEFAULT_SKIP_TABLES|CONFIG_OVERRIDE_FORBIDDEN|SIZE_LIMIT_ADVICE|LOCKED_ERROR|EVENT_[A-Z_]+)=`).FindAllStringSubmatch(bash, -1)
	require.NotEmpty(t, names)
	for _, match := range names {
		name := match[1]
		want := regexp.MustCompile(`(?m)^` + name + `='?(.*?)'?$`).FindStringSubmatch(bash)
		got := regexp.MustCompile(`(?m)^` + name + ` ?= ?r?['"]?(.*?)['"]?$`).FindStringSubmatch(python)
		// What a variable of the state of the bash script starts with, and what the Python one spells otherwise
		if slices.Contains([]string{"IN_PROGRESS_RE", "SIZE_LIMIT_ADVICE", "EVENT_FAILURES", "SLOT_TOLERANCE"}, name) {
			continue
		}
		require.NotNil(t, got, "the Python orchestrator defines no %s", name)
		require.Equal(t, want[1], got[1], name)
	}
}
