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

package k8s

import (
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
)

const sha256Digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestImageGetBaseName(t *testing.T) {
	for image, want := range map[string]string{
		"clickhouse/clickhouse-server:25.3":                       "clickhouse-server",
		"altinity/clickhouse-server:25.8.28.10001.altinitystable": "clickhouse-server",
		"registry.example.com:5000/mirror/clickhouse-server:24.8": "clickhouse-server",
		"clickhouse/clickhouse-server@sha256:" + sha256Digest:     "clickhouse-server",
		"busybox": "busybox",
		"123456789012.dkr.ecr.us-east-1.amazonaws.com/images:clickhouse": "images",
	} {
		got, ok := ImageGetBaseName(image)
		require.True(t, ok, image)
		require.Equal(t, want, got, image)
	}

	for _, image := range []string{"", "Upper/Case:1"} {
		_, ok := ImageGetBaseName(image)
		require.False(t, ok, "%q is not an image reference", image)
	}
}

func TestPodSpecContainerGet(t *testing.T) {
	spec := &core.PodSpec{Containers: []core.Container{{Name: "sidecar"}, {Name: "clickhouse"}}}

	got, ok := PodSpecContainerGet(spec, "clickhouse", 0)
	require.True(t, ok)
	require.Equal(t, "clickhouse", got.Name, "a name match wins over the index fallback")

	got, ok = PodSpecContainerGet(spec, "missing", 0)
	require.True(t, ok)
	require.Equal(t, "sidecar", got.Name, "the index is the fallback")

	_, ok = PodSpecContainerGet(&core.PodSpec{}, "clickhouse", 0)
	require.False(t, ok)
}

func TestPodSpecContainerGetOrFirstOther(t *testing.T) {
	for name, tc := range map[string]struct {
		containers []string
		// want is the index of the container found, -1 for none
		want int
	}{
		"by name, past another first":       {containers: []string{"server", "log", "clickhouse"}, want: 2},
		"the first other":                   {containers: []string{"log", "backup", "server", "proxy"}, want: 2},
		"the first, one of the others next": {containers: []string{"server", "log"}, want: 0},
		"none but the others":               {containers: []string{"log", "backup"}, want: -1},
		"no container":                      {want: -1},
	} {
		t.Run(name, func(t *testing.T) {
			spec := &core.PodSpec{}
			for _, container := range tc.containers {
				spec.Containers = append(spec.Containers, core.Container{Name: container})
			}
			got, ok := PodSpecContainerGetOrFirstOther(spec, "clickhouse", "log", "backup")
			require.Equal(t, tc.want >= 0, ok)
			if ok {
				require.Same(t, &spec.Containers[tc.want], got)
			}
		})
	}
}

// lookupIn finds a variable in env, as kubelet finds one among the env vars of a container
func lookupIn(env map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	}
}

// Kubelet reduces "$$" to "$" and replaces a $(VAR_NAME) reference to a variable of the container, leaving any other
// "$" - a reference to no variable, an unclosed one, a shell's own "$x" - as it is.
func TestExpandVariableReferences(t *testing.T) {
	env := lookupIn(map[string]string{"PORT": "7171", "EMPTY": ""})
	for in, want := range map[string]string{
		"plain":                  "plain",
		"$$":                     "$",
		"pid $$ and $$$":         "pid $ and $$",
		"$(PORT)":                "7171",
		"port $(PORT)/$(EMPTY)x": "port 7171/x",
		"$(UNSET) stays":         "$(UNSET) stays",
		"$(date +%s)":            "$(date +%s)",
		"$$(PORT) is escaped":    "$(PORT) is escaped",
		"unclosed $(PORT":        "unclosed $(PORT",
		"${PORT} and $PORT":      "${PORT} and $PORT",
		"trailing $":             "trailing $",
	} {
		require.Equal(t, want, ExpandVariableReferences(in, env), in)
	}
}

// A script escaped for the command of a container reaches it as written, whatever variables the container has: a
// script reading /proc/$$/environ must not read /proc/$/environ.
func TestEscapeVariableReferencesRoundTrips(t *testing.T) {
	env := lookupIn(map[string]string{"PORT": "7171", "HOME": "/root"})
	for _, script := range []string{
		"done </proc/$$/environ",
		"echo $(PORT) ${PORT} $PORT $(date +%s) $$(PORT) $",
		"unclosed $(HOME",
		"",
	} {
		require.Equal(t, script, ExpandVariableReferences(EscapeVariableReferences(script), env), script)
	}
	require.Equal(t, []string{"bash", "-c", "a $$ b", "$$(PORT)"}, EscapeVariableReferencesAll([]string{"bash", "-c", "a $ b", "$(PORT)"}))
}
