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
