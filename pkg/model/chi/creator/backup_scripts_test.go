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
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/model/chi/backupconfig"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
)

func requireScriptTools(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"bash", "env", "sleep", "touch", "printf"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is required to run the sidecar script: %v", tool, err)
		}
	}
	if _, err := os.Stat("/proc/self/environ"); err != nil {
		t.Skipf("the sidecar script reads its environment from /proc: %v", err)
	}
}

// writeConfigFile writes a configuration file of the sidecar, returning its path
func writeConfigFile(t *testing.T) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), backupconfig.FileName)
	require.NoError(t, os.WriteFile(file, []byte("api:\n  username: clickhouse_backup\n"), 0o644))
	return file
}

// The sidecar's command names the configuration file clickhouse-backup reads by default and the fingerprint file of
// the credentials of its REST API, in the directory the configuration file Secret is mounted at
func TestBackupSidecarCommandNamesTheConfigFile(t *testing.T) {
	command := newBackupSidecarCommand()
	require.Equal(t, []string{
		path.Join(config.DirPathClickHouseBackupConfig, backupconfig.FileName),
		path.Join(config.DirPathClickHouseBackupConfig, backupconfig.APIFingerprintFileName),
		config.ClickHouseBackupCommand,
		config.ClickHouseBackupCommandServer,
	}, command[4:])
}

// The pod template carries the script without its lines of comments and blanks, and the tests of the script run
// it so. A '#' within a line of code - a parameter expansion, a trailing comment - stays, as does a shebang. A
// heredoc would lose its comment and blank lines alike, so the script has none.
func TestStripScriptComments(t *testing.T) {
	require.Equal(t,
		"#!/bin/bash\nset -eu\n\techo \"${#x} ${y##*/}\" # count\n\tdone <<<\"${z}\"\n",
		stripScriptComments("#!/bin/bash\n# leading\n\nset -eu\n\t# indented\n\techo \"${#x} ${y##*/}\" # count\n  \n\tdone <<<\"${z}\"\n"))

	require.NotRegexp(t, `<<-?\s*['"]?\w`, backupSidecarScriptSource, "a heredoc would lose lines")
	require.Equal(t, stripScriptComments(backupSidecarScriptSource), backupSidecarScript)
	for _, line := range strings.Split(strings.TrimSuffix(backupSidecarScript, "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		require.False(t, (trimmed == "") || strings.HasPrefix(trimmed, "#"), "the sidecar script ships the line %q", line)
	}
}

// lockedOutput collects the output of a process that may still be writing it while it is read
type lockedOutput struct {
	mu  sync.Mutex
	out strings.Builder
}

func (o *lockedOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.out.Write(p)
}

func (o *lockedOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.out.String()
}

// sidecarScript is the sidecar's command script running with an environment
type sidecarScript struct {
	cmd  *exec.Cmd
	out  *lockedOutput
	done chan error
}

// startSidecarScript starts the sidecar's command script with the environment and the configuration file, running
// the command after it. The fingerprint file is the one next to the configuration file.
func startSidecarScript(t *testing.T, env map[string]string, configFile string, command ...string) *sidecarScript {
	t.Helper()
	fingerprintFile := filepath.Join(filepath.Dir(configFile), backupconfig.APIFingerprintFileName)
	args := append([]string{"-c", backupSidecarScript, config.ClickHouseBackupContainerName, configFile, fingerprintFile}, command...)
	s := &sidecarScript{cmd: exec.Command("bash", args...), out: &lockedOutput{}, done: make(chan error, 1)}
	s.cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	for name, value := range env {
		s.cmd.Env = append(s.cmd.Env, name+"="+value)
	}
	s.cmd.Stdout, s.cmd.Stderr = s.out, s.out
	require.NoError(t, s.cmd.Start())
	t.Cleanup(func() { _ = s.cmd.Process.Kill() })
	go func() { s.done <- s.cmd.Wait() }()
	return s
}

// wait waits for the script to exit, reporting whether it did in time and with which error
func (s *sidecarScript) wait(timeout time.Duration) (bool, error) {
	select {
	case err := <-s.done:
		return true, err
	case <-time.After(timeout):
		return false, nil
	}
}

// The sidecar's command starts the server with its arguments as they are, without the Kubernetes service links that
// clickhouse-backup cannot parse: a Service named "sftp" sets SFTP_PORT to tcp://<ip>:<port> in the namespace, and
// clickhouse-backup refuses its whole configuration over it. A port the spec sets is kept.
func TestBackupSidecarScriptStartsServer(t *testing.T) {
	requireScriptTools(t)

	for name, tc := range map[string]struct {
		env  map[string]string
		want []string
		gone []string
	}{
		"service links": {
			env: map[string]string{
				"SFTP_PORT":        "tcp://10.96.0.22:22",
				"KUBERNETES_PORT":  "tcp://10.96.0.1:443",
				"MINIO_PORT":       "udp://10.96.0.9:9000",
				"SFTP_PORT_22_TCP": "tcp://10.96.0.22:22",
			},
			want: []string{"SFTP_PORT_22_TCP=tcp://10.96.0.22:22"},
			gone: []string{"SFTP_PORT", "KUBERNETES_PORT", "MINIO_PORT"},
		},
		"ports set": {
			env: map[string]string{
				"SFTP_PORT":                              "2222",
				config.ClickHouseBackupEnvClickHousePort: "9000",
			},
			want: []string{"SFTP_PORT=2222", config.ClickHouseBackupEnvClickHousePort + "=9000"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			script := startSidecarScript(t, maps.Clone(tc.env), writeConfigFile(t), "env")
			exited, err := script.wait(10 * time.Second)
			require.True(t, exited)
			require.NoError(t, err, script.out.String())
			lines := strings.Split(script.out.String(), "\n")
			for _, line := range tc.want {
				require.Contains(t, lines, line)
			}
			for _, name := range tc.gone {
				for _, line := range lines {
					require.False(t, strings.HasPrefix(line, name+"="), "%s reached the server: %s", name, line)
				}
			}
		})
	}

	t.Run("arguments as they are", func(t *testing.T) {
		script := startSidecarScript(t, nil, writeConfigFile(t), "printf", `%s|`, "a b", "$(echo injected)")
		exited, err := script.wait(10 * time.Second)
		require.True(t, exited)
		require.NoError(t, err)
		require.Equal(t, "a b|$(echo injected)|", script.out.String())
	})
}

// The REST API lets in whoever presents its credentials, so everybody while they are missing: started before the
// configuration file that holds them is there, as its Secret did not exist yet, the sidecar's command does not run
// the server but idles, which leaves the pod ready, until kubelet puts the file in place, or stops the sidecar.
func TestBackupSidecarScriptWaitsForTheConfigFile(t *testing.T) {
	requireScriptTools(t)

	for name, empty := range map[string]bool{"missing": false, "empty": true} {
		t.Run(name, func(t *testing.T) {
			configFile := filepath.Join(t.TempDir(), backupconfig.FileName)
			if empty {
				require.NoError(t, os.WriteFile(configFile, nil, 0o644))
			}
			started := filepath.Join(t.TempDir(), "started")

			t.Run("until it is there", func(t *testing.T) {
				script := startSidecarScript(t, nil, configFile, "touch", started)
				exited, _ := script.wait(time.Second)
				require.False(t, exited, "the sidecar must keep running: %s", script.out.String())
				require.NoFileExists(t, started, "the server must not start without the configuration file")
				require.Contains(t, script.out.String(), "the REST API is not served")

				require.NoError(t, os.WriteFile(configFile, []byte("api: {}\n"), 0o644))
				exited, err := script.wait(15 * time.Second)
				require.True(t, exited, "the server must start once the configuration file is there")
				require.NoError(t, err, script.out.String())
				require.FileExists(t, started)
			})
		})
	}

	t.Run("until it is stopped", func(t *testing.T) {
		started := filepath.Join(t.TempDir(), "started")
		script := startSidecarScript(t, nil, filepath.Join(t.TempDir(), backupconfig.FileName), "touch", started)
		exited, _ := script.wait(time.Second)
		require.False(t, exited, "the sidecar must keep running: %s", script.out.String())

		require.NoError(t, script.cmd.Process.Signal(syscall.SIGTERM))
		exited, err := script.wait(5 * time.Second)
		require.True(t, exited, "the idling sidecar must stop when kubelet stops it")
		require.NoError(t, err, script.out.String())
		require.NoFileExists(t, started)
	})
}

// The sidecar's command takes the configuration file, the fingerprint file and the command of the server as
// arguments from the operator; anything else is the operator's own fault and fails the sidecar loudly.
func TestBackupSidecarScriptArguments(t *testing.T) {
	requireScriptTools(t)

	configFile := path.Join(config.DirPathClickHouseBackupConfig, backupconfig.FileName)
	fingerprintFile := path.Join(config.DirPathClickHouseBackupConfig, backupconfig.APIFingerprintFileName)
	for name, args := range map[string][]string{
		"no arguments":             {},
		"no fingerprint file":      {configFile},
		"no command":               {configFile, fingerprintFile},
		"configuration file empty": {"", fingerprintFile, "env"},
		"fingerprint file empty":   {configFile, "", "env"},
	} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command("bash", append([]string{"-c", backupSidecarScript, config.ClickHouseBackupContainerName}, args...)...)
			cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
			out, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, string(out))
			require.Equal(t, 1, exitErr.ExitCode(), string(out))
			require.Contains(t, string(out), "the configuration file, the fingerprint file and the command of the server must be given")
		})
	}
}

// fakeServer stands for clickhouse-backup server: it reports every SIGHUP and exits on SIGTERM
var fakeServer = []string{"bash", "-c", `trap 'echo reloaded' HUP; trap 'echo stopped; exit 0' TERM; while :; do sleep 0.1; done`}

// clickhouse-backup checks the credentials of a request against the configuration it last loaded, which only a
// request it lets in loads anew: the sidecar's command restarts the server by SIGHUP when the fingerprint of the
// credentials changes, and only then, and passes SIGTERM on to it.
func TestBackupSidecarScriptRestartsServerOnNewCredentials(t *testing.T) {
	requireScriptTools(t)

	configFile := writeConfigFile(t)
	fingerprintFile := filepath.Join(filepath.Dir(configFile), backupconfig.APIFingerprintFileName)
	require.NoError(t, os.WriteFile(fingerprintFile, []byte("first"), 0o644))
	script := startSidecarScript(t, nil, configFile, fakeServer...)

	exited, _ := script.wait(7 * time.Second)
	require.False(t, exited, script.out.String())
	require.NotContains(t, script.out.String(), "reloaded", "unchanged credentials do not restart the server")

	require.NoError(t, os.WriteFile(fingerprintFile, []byte("second"), 0o644))
	require.Eventually(t, func() bool { return strings.Contains(script.out.String(), "reloaded") }, 15*time.Second, 100*time.Millisecond)
	require.Equal(t, 1, strings.Count(script.out.String(), "reloaded"), script.out.String())

	require.NoError(t, os.Remove(fingerprintFile))
	exited, _ = script.wait(7 * time.Second)
	require.False(t, exited, script.out.String())
	require.Equal(t, 1, strings.Count(script.out.String(), "reloaded"), "a fingerprint file gone restarts nothing")

	require.NoError(t, script.cmd.Process.Signal(syscall.SIGTERM))
	exited, err := script.wait(10 * time.Second)
	require.True(t, exited, "the sidecar must stop when kubelet stops it")
	require.NoError(t, err, script.out.String())
	require.Contains(t, script.out.String(), "stopped", "SIGTERM reaches the server")
}

// The sidecar exits as the server does
func TestBackupSidecarScriptExitsAsTheServer(t *testing.T) {
	requireScriptTools(t)

	script := startSidecarScript(t, nil, writeConfigFile(t), "bash", "-c", "exit 3")
	exited, err := script.wait(10 * time.Second)
	require.True(t, exited)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, script.out.String())
	require.Equal(t, 3, exitErr.ExitCode())
}

// The sidecar runs the script in the image of clickhouse-backup, an Alpine one, whose regex library refuses a
// repetition count above 255, the least RE_DUP_MAX POSIX allows.
func TestBackupSidecarScriptRegexesArePortable(t *testing.T) {
	counts := regexp.MustCompile(`\{([0-9]*),?([0-9]*)\}`)
	for _, count := range counts.FindAllStringSubmatch(backupSidecarScript, -1) {
		for _, bound := range count[1:] {
			if bound == "" {
				continue
			}
			n, err := strconv.Atoi(bound)
			require.NoError(t, err)
			require.LessOrEqual(t, n, 255, "repetition count %s", count[0])
		}
	}
}
