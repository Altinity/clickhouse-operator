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
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
)

const (
	scriptTestAPIUsername = "clickhouse_backup"
	// Colon, quotes and a command substitution must all survive untouched
	scriptTestAPIPassword = `p:a"s's$(touch /tmp/x)`
)

func requireScriptTools(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"bash", "curl", "base64", "tr", "env", "sleep"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is required to run the sidecar scripts: %v", tool, err)
		}
	}
	if _, err := os.Stat("/proc/self/environ"); err != nil {
		t.Skipf("the liveness probe reads its environment from /proc: %v", err)
	}
}

// mountSecret lays the Secret out as kubelet does: the files in a timestamped directory, the "..data" link to it, and
// a link per key into "..data". A Secret that does not exist is mounted as one without keys.
func mountSecret(t *testing.T, data map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	timestamped := filepath.Join(dir, "..2026_10_05_12_00_00.123456789")
	require.NoError(t, os.Mkdir(timestamped, 0o755))
	for key, value := range data {
		require.NoError(t, os.WriteFile(filepath.Join(timestamped, key), []byte(value), 0o644))
	}
	require.NoError(t, os.Symlink(filepath.Base(timestamped), filepath.Join(dir, "..data")))
	for key := range data {
		require.NoError(t, os.Symlink(filepath.Join("..data", key), filepath.Join(dir, key)))
	}
	return dir
}

// actionTimestamp is the time an action of clickhouse-backup started or finished, in its format; the probe runs in UTC
func actionTimestamp(at time.Time) string {
	return at.UTC().Format(time.DateTime)
}

// actionsStub serves /backup/actions of the sidecar REST API
type actionsStub struct {
	mu sync.Mutex
	// busy makes the sidecar report an operation in progress
	busy bool
	// lastFinish is when the last action finished, an hour ago when zero
	lastFinish time.Time
	// status, when set, answers every request with it
	status int
	// authorized counts the requests with the expected credentials
	authorized int
	requests   int
}

func (s *actionsStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests++
	user, pass, ok := r.BasicAuth()
	if !ok || (user != scriptTestAPIUsername) || (pass != scriptTestAPIPassword) || (r.URL.Path != "/backup/actions") {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	s.authorized++
	if s.status != 0 {
		w.WriteHeader(s.status)
		return
	}
	lastFinish := s.lastFinish
	if lastFinish.IsZero() {
		lastFinish = time.Now().Add(-time.Hour)
	}
	w.Header().Set("Content-Type", "application/json")
	rows := []map[string]string{
		{"command": "create_remote nightly", "status": "success", "start": actionTimestamp(lastFinish.Add(-2 * time.Hour)), "finish": actionTimestamp(lastFinish.Add(-time.Hour))},
		{"command": "list remote", "status": "success", "start": actionTimestamp(lastFinish.Add(-time.Second)), "finish": actionTimestamp(lastFinish)},
	}
	if s.busy {
		rows = append(rows, map[string]string{"command": "restore_remote nightly", "status": "in progress", "start": actionTimestamp(lastFinish), "operation_id": "1"})
	}
	for _, row := range rows {
		out, _ := json.Marshal(row)
		_, _ = w.Write(append(out, '\n'))
	}
}

// probeFixture is what a liveness probe of the sidecar finds
type probeFixture struct {
	// credentials and storage are the Secrets mounted for the probe, nil when not mounted
	credentials map[string]string
	storage     map[string]string
	// env is the environment the sidecar started with
	env map[string]string
	// explicit are the env vars the sidecar sets itself
	explicit []string
	stub     actionsStub
	// apiDown makes the REST API unreachable
	apiDown bool
	// curlrc is the curl config file the probe finds, none when empty
	curlrc string
}

// newProbeFixture is a sidecar that started with the Secrets as they are now. Values with trailing newlines, many
// lines, quotes and names the shell sets itself are all compared as they are.
func newProbeFixture() *probeFixture {
	credentials := map[string]string{
		config.ClickHouseBackupSecretKeyAPIUsername:        scriptTestAPIUsername,
		config.ClickHouseBackupSecretKeyAPIPassword:        scriptTestAPIPassword,
		config.ClickHouseBackupSecretKeyClickHouseUsername: config.ClickHouseBackupUsername,
		config.ClickHouseBackupSecretKeyClickHousePassword: "ch-password",
	}
	storage := map[string]string{
		"REMOTE_STORAGE":       "gcs",
		"GCS_CREDENTIALS_JSON": "{\n  \"type\": \"service_account\"\n}\n",
		"S3_SECRET_KEY":        "key\n\n",
		"PWD":                  "/elsewhere",
		"SHLVL":                "7",
		// The operator sets these itself, overriding the Secret
		config.ClickHouseBackupEnvBackupsToKeepLocal: "3",
		config.ClickHouseBackupSecretKeyAPIPassword:  "not the API password",
	}
	// The env vars the sidecar sets itself win over the keys of the storage Secret
	env := maps.Clone(storage)
	maps.Copy(env, credentials)
	env[config.ClickHouseBackupEnvBackupsToKeepLocal] = config.ClickHouseBackupBackupsToKeepLocalDeleteAfterUpload
	env["PATH"] = os.Getenv("PATH")
	return &probeFixture{
		credentials: credentials,
		storage:     storage,
		env:         env,
		explicit: []string{
			config.ClickHouseBackupEnvAPIListen,
			config.ClickHouseBackupSecretKeyAPIUsername,
			config.ClickHouseBackupSecretKeyAPIPassword,
			config.ClickHouseBackupSecretKeyClickHouseUsername,
			config.ClickHouseBackupSecretKeyClickHousePassword,
			config.ClickHouseBackupEnvBackupsToKeepLocal,
		},
	}
}

// run runs the liveness probe, returning its exit code and output
func (f *probeFixture) run(t *testing.T) (int, string) {
	t.Helper()
	server := httptest.NewServer(&f.stub)
	t.Cleanup(server.Close)
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	require.NoError(t, err)
	if f.apiDown {
		server.Close()
	}
	env := maps.Clone(f.env)
	env[config.ClickHouseBackupEnvAPIListen] = net.JoinHostPort(config.ClickHouseBackupAPIListenHost, port)
	env["TZ"] = "UTC"
	if f.curlrc != "" {
		home := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(home, ".curlrc"), []byte(f.curlrc), 0o644))
		env["CURL_HOME"] = home
	}

	dirs := []string{"", ""}
	for i, secret := range []map[string]string{f.credentials, f.storage} {
		if secret != nil {
			dirs[i] = mountSecret(t, secret)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args := append([]string{"-c", backupSidecarLivenessScript, config.ClickHouseBackupContainerName}, newBackupLivenessArgs(dirs[0], dirs[1])...)
	cmd := exec.CommandContext(ctx, "bash", append(args, f.explicit...)...)
	for name, value := range env {
		cmd.Env = append(cmd.Env, name+"="+value)
	}
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), string(out)
	}
	require.NoError(t, err, string(out))
	return 0, string(out)
}

// The liveness probe of the sidecar fails, so that kubelet restarts the sidecar alone, once a mounted Secret holds a
// value the sidecar did not start with - but not while the sidecar runs an operation or has finished one within the
// quiet window, as a restart would break the operation or the Job about to take its next step, and not while the
// backup Secret is missing, without which the restarted sidecar would lack its credentials. A sidecar whose REST API
// does not answer runs nothing a Job drives and is restarted. The probe asks the sidecar itself, whatever proxy its
// environment or a curl config names.
func TestBackupSidecarLivenessProbe(t *testing.T) {
	requireScriptTools(t)
	quietWindow := time.Duration(backupLivenessQuietSeconds) * time.Second
	const unreachableProxy = "http://127.0.0.1:1"

	for name, tc := range map[string]struct {
		mutate func(f *probeFixture)
		// wantRestart tells whether the probe fails
		wantRestart bool
		// wantChanged is the file the probe reports changed
		wantChanged string
		// wantAsked tells whether the probe asked the REST API with the sidecar's credentials
		wantAsked bool
	}{
		"unchanged": {
			mutate: func(f *probeFixture) {},
		},
		"backup Secret changed": {
			mutate: func(f *probeFixture) {
				f.credentials[config.ClickHouseBackupSecretKeyClickHousePassword] = "regenerated"
			},
			wantRestart: true,
			wantChanged: config.ClickHouseBackupSecretKeyClickHousePassword,
			wantAsked:   true,
		},
		"storage Secret changed": {
			mutate:      func(f *probeFixture) { f.storage["S3_SECRET_KEY"] = "key\n" },
			wantRestart: true,
			wantChanged: "S3_SECRET_KEY",
			wantAsked:   true,
		},
		"storage Secret gained the path the sidecar defaults": {
			mutate:      func(f *probeFixture) { f.storage["GCS_PATH"] = "backups/{shard}" },
			wantRestart: true,
			wantChanged: "GCS_PATH",
			wantAsked:   true,
		},
		"storage Secret gained a key": {
			mutate:      func(f *probeFixture) { f.storage["S3_REGION"] = "us-east-1" },
			wantRestart: true,
			wantChanged: "S3_REGION",
			wantAsked:   true,
		},
		"storage Secret created after the sidecar started": {
			mutate: func(f *probeFixture) {
				for key := range f.storage {
					if !strings.HasPrefix(key, "API_") && (key != config.ClickHouseBackupEnvBackupsToKeepLocal) {
						delete(f.env, key)
					}
				}
			},
			wantRestart: true,
			wantAsked:   true,
		},
		"key the sidecar sets itself changed": {
			mutate: func(f *probeFixture) { f.storage[config.ClickHouseBackupEnvBackupsToKeepLocal] = "5" },
		},
		"keys that are no settings": {
			mutate: func(f *probeFixture) {
				f.storage["s3.endpoint"] = "anything"
				f.storage["9_LIVES"] = "anything"
			},
		},
		"storage Secret not followed": {
			mutate: func(f *probeFixture) {
				f.storage["S3_SECRET_KEY"] = "rotated"
				f.storage, f.explicit = nil, nil
			},
		},
		"storage Secret deleted": {
			mutate: func(f *probeFixture) { f.storage = map[string]string{} },
		},
		"backup Secret deleted": {
			mutate: func(f *probeFixture) {
				f.credentials = map[string]string{}
				f.storage["S3_SECRET_KEY"] = "rotated"
			},
		},
		"backup Secret not followed": {
			mutate: func(f *probeFixture) {
				f.credentials = nil
				f.storage["S3_SECRET_KEY"] = "rotated"
			},
			wantRestart: true,
			wantChanged: "S3_SECRET_KEY",
			wantAsked:   true,
		},
		"operation in progress": {
			mutate: func(f *probeFixture) {
				f.storage["S3_SECRET_KEY"] = "rotated"
				f.stub.busy = true
			},
			wantChanged: "S3_SECRET_KEY",
			wantAsked:   true,
		},
		"operation finished just now": {
			mutate: func(f *probeFixture) {
				f.storage["S3_SECRET_KEY"] = "rotated"
				f.stub.lastFinish = time.Now().Add(-10 * time.Second)
			},
			wantChanged: "S3_SECRET_KEY",
			wantAsked:   true,
		},
		"operation finished late in the quiet window": {
			mutate: func(f *probeFixture) {
				f.storage["S3_SECRET_KEY"] = "rotated"
				f.stub.lastFinish = time.Now().Add(-quietWindow + 10*time.Second)
			},
			wantChanged: "S3_SECRET_KEY",
			wantAsked:   true,
		},
		"operation finished past the quiet window": {
			mutate: func(f *probeFixture) {
				f.storage["S3_SECRET_KEY"] = "rotated"
				f.stub.lastFinish = time.Now().Add(-quietWindow - 10*time.Second)
			},
			wantRestart: true,
			wantChanged: "S3_SECRET_KEY",
			wantAsked:   true,
		},
		"operation in progress, http_proxy set": {
			mutate: func(f *probeFixture) {
				f.storage["S3_SECRET_KEY"] = "rotated"
				f.env["http_proxy"] = unreachableProxy
				f.stub.busy = true
			},
			wantChanged: "S3_SECRET_KEY",
			wantAsked:   true,
		},
		"operation in progress, ALL_PROXY set": {
			mutate: func(f *probeFixture) {
				f.storage["S3_SECRET_KEY"] = "rotated"
				f.env["ALL_PROXY"] = unreachableProxy
				f.stub.busy = true
			},
			wantChanged: "S3_SECRET_KEY",
			wantAsked:   true,
		},
		"operation in progress, curl config discards the answer": {
			mutate: func(f *probeFixture) {
				f.storage["S3_SECRET_KEY"] = "rotated"
				f.curlrc = "output = \"/dev/null\"\n"
				f.stub.busy = true
			},
			wantChanged: "S3_SECRET_KEY",
			wantAsked:   true,
		},
		"operation finished in another time zone": {
			mutate: func(f *probeFixture) {
				f.storage["S3_SECRET_KEY"] = "rotated"
				f.stub.lastFinish = time.Now().Add(2 * time.Hour)
			},
			wantRestart: true,
			wantChanged: "S3_SECRET_KEY",
			wantAsked:   true,
		},
		"REST API unreachable": {
			mutate: func(f *probeFixture) {
				f.storage["S3_SECRET_KEY"] = "rotated"
				f.apiDown = true
			},
			wantRestart: true,
			wantChanged: "S3_SECRET_KEY",
		},
		"REST API failing": {
			mutate: func(f *probeFixture) {
				f.storage["S3_SECRET_KEY"] = "rotated"
				f.stub.status = http.StatusInternalServerError
			},
			wantRestart: true,
			wantChanged: "S3_SECRET_KEY",
			wantAsked:   true,
		},
		"started while the backup Secret was missing": {
			mutate: func(f *probeFixture) {
				for key := range f.credentials {
					delete(f.env, key)
				}
			},
			wantRestart: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newProbeFixture()
			tc.mutate(f)
			code, out := f.run(t)
			if tc.wantRestart {
				require.Equal(t, 1, code, out)
				require.Contains(t, out, "restarting it to apply the change")
			} else {
				require.Equal(t, 0, code, out)
			}
			if tc.wantChanged != "" {
				require.Contains(t, out, "/"+tc.wantChanged+" changed since the sidecar started")
			}
			if !tc.wantRestart && (tc.wantChanged != "") {
				require.Contains(t, out, "it restarts once it has run no operation for a while")
			}
			if tc.wantAsked {
				require.Positive(t, f.stub.authorized, "the probe did not ask the REST API with the sidecar's credentials")
			} else if !tc.wantRestart {
				require.Zero(t, f.stub.requests, "an unchanged Secret needs no call of the REST API")
			}
		})
	}
}

// The liveness probe takes its timing as arguments from the operator; anything else is the operator's own fault and
// fails the probe loudly rather than computing with it.
func TestBackupSidecarLivenessProbeTimingArguments(t *testing.T) {
	requireScriptTools(t)

	for name, args := range map[string][]string{
		"API timeout not a number":  {"5s", "60"},
		"quiet window not a number": {"5", "$(touch /tmp/x)"},
		"quiet window empty":        {"5", ""},
	} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command("bash", append([]string{"-c", backupSidecarLivenessScript, config.ClickHouseBackupContainerName}, append(args, "", "")...)...)
			cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
			out, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, string(out))
			require.Equal(t, 1, exitErr.ExitCode(), string(out))
			require.Contains(t, string(out), "must be numbers of seconds")
		})
	}
}

// The pod template mounts a file of the backup Secret for exactly the env vars the probe compares: those named as a
// shell identifier
func TestBackupSidecarLivenessProbeIdentifiers(t *testing.T) {
	match := regexp.MustCompile(`(?m)^IDENTIFIER_RE='([^']*)'$`).FindStringSubmatch(backupSidecarLivenessScript)
	require.NotNil(t, match, "the probe no longer defines IDENTIFIER_RE")
	require.Equal(t, envVarIdentifierRE.String(), match[1])
}

// sidecarDefaultConfigFileLine is the line of the sidecar's command that names the configuration file clickhouse-backup
// reads by default
var sidecarDefaultConfigFileLine = regexp.MustCompile(`(?m)^DEFAULT_CONFIG_FILE=(\S+)$`)

// The sidecar looks for the configuration file clickhouse-backup reads by default in the directory a mount of the pod
// template is checked against
func TestBackupSidecarDefaultConfigFile(t *testing.T) {
	match := sidecarDefaultConfigFileLine.FindStringSubmatch(backupSidecarScriptSource)
	require.NotNil(t, match, "the sidecar script no longer defines DEFAULT_CONFIG_FILE")
	require.Equal(t, config.DirPathClickHouseBackupConfig, filepath.Dir(match[1]))
}

// The pod template carries the scripts without their lines of comments and blanks, and the tests of the scripts run
// them so. A '#' within a line of code - a parameter expansion, a trailing comment - stays, as does a shebang. A
// heredoc would lose its comment and blank lines alike, so the scripts have none.
func TestStripScriptComments(t *testing.T) {
	require.Equal(t,
		"#!/bin/bash\nset -eu\n\techo \"${#x} ${y##*/}\" # count\n\tdone <<<\"${z}\"\n",
		stripScriptComments("#!/bin/bash\n# leading\n\nset -eu\n\t# indented\n\techo \"${#x} ${y##*/}\" # count\n  \n\tdone <<<\"${z}\"\n"))

	for name, tc := range map[string]struct{ source, shipped string }{
		"sidecar":        {backupSidecarScriptSource, backupSidecarScript},
		"liveness probe": {backupSidecarLivenessScriptSource, backupSidecarLivenessScript},
	} {
		require.NotRegexp(t, `<<-?\s*['"]?\w`, tc.source, "%s: a heredoc would lose lines", name)
		require.Equal(t, stripScriptComments(tc.source), tc.shipped, name)
		for _, line := range strings.Split(strings.TrimSuffix(tc.shipped, "\n"), "\n") {
			trimmed := strings.TrimSpace(line)
			require.False(t, (trimmed == "") || strings.HasPrefix(trimmed, "#"), "%s ships the line %q", name, line)
		}
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

// startSidecarScript starts the sidecar's command script with the environment and the operator's arguments, running
// the command after it
func startSidecarScript(t *testing.T, env map[string]string, command ...string) *sidecarScript {
	t.Helper()
	return startScript(t, backupSidecarScript, env, command...)
}

// startScript is startSidecarScript of the sidecar's command given as script
func startScript(t *testing.T, script string, env map[string]string, command ...string) *sidecarScript {
	t.Helper()
	args := append([]string{"-c", script, config.ClickHouseBackupContainerName}, append(newBackupSidecarArgs(), command...)...)
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
// clickhouse-backup refuses its whole configuration over it. A port the Secret or the spec sets is kept.
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
			env := map[string]string{
				config.ClickHouseBackupSecretKeyAPIUsername: scriptTestAPIUsername,
				config.ClickHouseBackupSecretKeyAPIPassword: scriptTestAPIPassword,
			}
			maps.Copy(env, tc.env)
			script := startSidecarScript(t, env, "env")
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
			require.Contains(t, lines, config.ClickHouseBackupSecretKeyAPIPassword+"="+scriptTestAPIPassword)
		})
	}

	t.Run("arguments as they are", func(t *testing.T) {
		script := startSidecarScript(t, map[string]string{
			config.ClickHouseBackupSecretKeyAPIUsername: scriptTestAPIUsername,
			config.ClickHouseBackupSecretKeyAPIPassword: scriptTestAPIPassword,
		}, "printf", `%s|`, "a b", "$(echo injected)")
		exited, err := script.wait(10 * time.Second)
		require.True(t, exited)
		require.NoError(t, err)
		require.Equal(t, "a b|$(echo injected)|", script.out.String())
	})
}

// The REST API lets in whoever presents its credentials, so everybody while they are empty: started without them,
// as the backup Secret was missing, the sidecar's command does not run the server but idles, which leaves the pod
// ready, until it is stopped.
func TestBackupSidecarScriptIdlesWithoutCredentials(t *testing.T) {
	requireScriptTools(t)

	for name, env := range map[string]map[string]string{
		"no credentials":  {},
		"no API password": {config.ClickHouseBackupSecretKeyAPIUsername: scriptTestAPIUsername},
		"empty password": {
			config.ClickHouseBackupSecretKeyAPIUsername: scriptTestAPIUsername,
			config.ClickHouseBackupSecretKeyAPIPassword: "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			started := filepath.Join(t.TempDir(), "started")
			script := startSidecarScript(t, env, "touch", started)
			exited, _ := script.wait(time.Second)
			require.False(t, exited, "the sidecar must keep running: %s", script.out.String())
			require.NoFileExists(t, started, "the server must not start without credentials")

			require.NoError(t, script.cmd.Process.Signal(syscall.SIGTERM))
			exited, err := script.wait(5 * time.Second)
			require.True(t, exited, "the idling sidecar must stop when kubelet stops it")
			require.NoError(t, err, script.out.String())
			require.Contains(t, script.out.String(), "the REST API is not served")
		})
	}
}

// serverEnv runs the sidecar's command, given as script, with the environment, returning the environment the server
// starts with
func serverEnv(t *testing.T, script string, env map[string]string) map[string]string {
	t.Helper()
	run := startScript(t, script, env, "env", "-0")
	exited, err := run.wait(10 * time.Second)
	require.True(t, exited)
	require.NoError(t, err, run.out.String())
	got := map[string]string{}
	for _, entry := range strings.Split(strings.TrimSuffix(run.out.String(), "\x00"), "\x00") {
		name, value, _ := strings.Cut(entry, "=")
		got[name] = value
	}
	return got
}

// The sidecar's command gives the storage REMOTE_STORAGE names the operator's default paths where its
// environment sets none - set to an empty value is set - and leaves the variables of other providers alone. Nothing is
// defaulted where a configuration file is in place, whose paths the env vars would take precedence over.
func TestBackupSidecarScriptDefaultsPaths(t *testing.T) {
	requireScriptTools(t)
	configFile := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(configFile, []byte("s3:\n  path: mine\n"), 0o644))
	const ownPath = "own/{shard}"
	defaulted := func(provider string) map[string]string {
		return map[string]string{
			provider + "_PATH":             chop.Config().Backup.StoragePath,
			provider + "_OBJECT_DISK_PATH": chop.Config().Backup.StorageObjectDiskPath,
		}
	}

	// withDefaultConfigFile is the sidecar's command where clickhouse-backup finds a configuration file by default, as
	// in an image that ships one
	withDefaultConfigFile := sidecarDefaultConfigFileLine.ReplaceAllLiteralString(backupSidecarScript, "DEFAULT_CONFIG_FILE='"+configFile+"'")
	require.NotEqual(t, backupSidecarScript, withDefaultConfigFile)

	for name, tc := range map[string]struct {
		env  map[string]string
		want map[string]string
		// gone are the variables the server must not get
		gone []string
		// defaultConfigFile puts a configuration file where clickhouse-backup reads one by default
		defaultConfigFile bool
	}{
		"s3":     {env: map[string]string{"REMOTE_STORAGE": "s3"}, want: defaulted("S3"), gone: []string{"GCS_PATH"}},
		"gcs":    {env: map[string]string{"REMOTE_STORAGE": "gcs"}, want: defaulted("GCS"), gone: []string{"S3_PATH"}},
		"azblob": {env: map[string]string{"REMOTE_STORAGE": "azblob"}, want: defaulted("AZBLOB")},
		"cos":    {env: map[string]string{"REMOTE_STORAGE": "cos"}, want: defaulted("COS")},
		"ftp":    {env: map[string]string{"REMOTE_STORAGE": "ftp"}, want: defaulted("FTP")},
		"sftp":   {env: map[string]string{"REMOTE_STORAGE": "sftp"}, want: defaulted("SFTP")},
		"path set": {
			env:  map[string]string{"REMOTE_STORAGE": "s3", "S3_PATH": ownPath},
			want: map[string]string{"S3_PATH": ownPath, "S3_OBJECT_DISK_PATH": chop.Config().Backup.StorageObjectDiskPath},
		},
		"path set empty": {
			env:  map[string]string{"REMOTE_STORAGE": "s3", "S3_PATH": ""},
			want: map[string]string{"S3_PATH": "", "S3_OBJECT_DISK_PATH": chop.Config().Backup.StorageObjectDiskPath},
		},
		"object disk path set": {
			env:  map[string]string{"REMOTE_STORAGE": "gcs", "GCS_OBJECT_DISK_PATH": ownPath},
			want: map[string]string{"GCS_PATH": chop.Config().Backup.StoragePath, "GCS_OBJECT_DISK_PATH": ownPath},
		},
		"another provider's path set": {
			env:  map[string]string{"REMOTE_STORAGE": "s3", "GCS_PATH": ownPath},
			want: map[string]string{"S3_PATH": chop.Config().Backup.StoragePath, "GCS_PATH": ownPath},
		},
		"no storage": {gone: []string{"S3_PATH", "S3_OBJECT_DISK_PATH"}},
		"storage none": {
			env:  map[string]string{"REMOTE_STORAGE": "none"},
			gone: []string{"NONE_PATH", "S3_PATH"},
		},
		"custom storage": {
			env:  map[string]string{"REMOTE_STORAGE": "custom"},
			gone: []string{"CUSTOM_PATH", "S3_PATH"},
		},
		"configuration file": {
			env:  map[string]string{"REMOTE_STORAGE": "s3", "CLICKHOUSE_BACKUP_CONFIG": configFile},
			gone: []string{"S3_PATH", "S3_OBJECT_DISK_PATH"},
		},
		"configuration file named but missing": {
			env:  map[string]string{"REMOTE_STORAGE": "s3", "CLICKHOUSE_BACKUP_CONFIG": configFile + ".missing"},
			want: defaulted("S3"),
		},
		"default configuration file": {
			env:               map[string]string{"REMOTE_STORAGE": "s3"},
			gone:              []string{"S3_PATH", "S3_OBJECT_DISK_PATH"},
			defaultConfigFile: true,
		},
		// clickhouse-backup reads no configuration file at all where the variable is set empty
		"configuration file set empty, beside a default one": {
			env:               map[string]string{"REMOTE_STORAGE": "s3", "CLICKHOUSE_BACKUP_CONFIG": ""},
			want:              defaulted("S3"),
			defaultConfigFile: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			env := map[string]string{
				config.ClickHouseBackupSecretKeyAPIUsername: scriptTestAPIUsername,
				config.ClickHouseBackupSecretKeyAPIPassword: scriptTestAPIPassword,
			}
			maps.Copy(env, tc.env)
			script := backupSidecarScript
			if tc.defaultConfigFile {
				script = withDefaultConfigFile
			}
			got := serverEnv(t, script, env)
			for name, value := range tc.want {
				require.Contains(t, got, name)
				require.Equal(t, value, got[name], name)
			}
			for _, name := range tc.gone {
				require.NotContains(t, got, name)
			}
			require.Equal(t, scriptTestAPIPassword, got[config.ClickHouseBackupSecretKeyAPIPassword])
		})
	}
}

// The sidecar's command takes the default paths and the command of the server as arguments from the operator;
// anything else is the operator's own fault and fails the sidecar loudly rather than defaulting a path to nothing.
func TestBackupSidecarScriptArguments(t *testing.T) {
	requireScriptTools(t)

	for name, args := range map[string][]string{
		"no arguments":            {},
		"no command":              newBackupSidecarArgs(),
		"storage path empty":      {"", chop.Config().Backup.StorageObjectDiskPath, "env"},
		"object disk path empty":  {chop.Config().Backup.StoragePath, "", "env"},
		"default paths left out":  {"env"},
		"one default path missed": {chop.Config().Backup.StoragePath, "env"},
	} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command("bash", append([]string{"-c", backupSidecarScript, config.ClickHouseBackupContainerName}, args...)...)
			cmd.Env = []string{
				"PATH=" + os.Getenv("PATH"),
				"REMOTE_STORAGE=s3",
				config.ClickHouseBackupSecretKeyAPIUsername + "=" + scriptTestAPIUsername,
				config.ClickHouseBackupSecretKeyAPIPassword + "=" + scriptTestAPIPassword,
			}
			out, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, string(out))
			require.Equal(t, 1, exitErr.ExitCode(), string(out))
			require.Contains(t, string(out), "the default storage paths and the command of the server must be given")
		})
	}
}

// The sidecar runs the scripts in the image of clickhouse-backup, an Alpine one, whose regex library refuses a
// repetition count above 255, the least RE_DUP_MAX POSIX allows.
func TestBackupSidecarScriptRegexesArePortable(t *testing.T) {
	counts := regexp.MustCompile(`\{([0-9]*),?([0-9]*)\}`)
	for name, script := range map[string]string{
		"sidecar":  backupSidecarScript,
		"liveness": backupSidecarLivenessScript,
	} {
		for _, count := range counts.FindAllStringSubmatch(script, -1) {
			for _, bound := range count[1:] {
				if bound == "" {
					continue
				}
				n, err := strconv.Atoi(bound)
				require.NoError(t, err)
				require.LessOrEqual(t, n, 255, "%s: repetition count %s", name, count[0])
			}
		}
	}
}
