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

package volume

import (
	"testing"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"

	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
)

func TestAppendHotReloadUsersVolumeUsesCanonicalName(t *testing.T) {
	sts := statefulSet("sidecar", "clickhouse")
	err := appendHotReloadUsersVolume(sts, "chi-demo-common-usersd", "chi-demo-users")
	require.NoError(t, err)

	sidecar := &sts.Spec.Template.Spec.Containers[0]
	server := &sts.Spec.Template.Spec.Containers[1]
	require.Equal(t, "chi-demo-common-usersd", sidecar.VolumeMounts[0].Name)
	require.Empty(t, sidecar.VolumeMounts[0].SubPath)
	require.Equal(t, "chi-demo-users", server.VolumeMounts[0].Name)
	require.Empty(t, server.VolumeMounts[0].SubPath)
	require.Equal(t, config.DirPathConfigUsers, server.VolumeMounts[0].MountPath)

	var projected *core.Volume
	for i := range sts.Spec.Template.Spec.Volumes {
		if sts.Spec.Template.Spec.Volumes[i].Name == "chi-demo-users" {
			projected = &sts.Spec.Template.Spec.Volumes[i]
		}
	}
	require.NotNil(t, projected.Projected)
	require.Equal(t, "chi-demo-users", projected.Projected.Sources[1].Secret.Name)
	require.Equal(t, config.ChopGeneratedHotReloadUsersConfigFilename(), projected.Projected.Sources[1].Secret.Items[0].Path)
	require.Equal(t, config.ChopGeneratedHotReloadUsersConfigFilename(), projected.Projected.Sources[1].Secret.Items[0].Key)
}

func TestAppendHotReloadUsersVolumeFallsBackToFirstContainer(t *testing.T) {
	sts := statefulSet("custom-server")
	err := appendHotReloadUsersVolume(sts, "users-cm", "users-secret")
	require.NoError(t, err)
	require.Equal(t, "users-secret", sts.Spec.Template.Spec.Containers[0].VolumeMounts[0].Name)
}

func TestAppendHotReloadUsersVolumeErrorsWhenMissing(t *testing.T) {
	err := appendHotReloadUsersVolume(&apps.StatefulSet{}, "users-cm", "users-secret")
	require.Error(t, err)
}

func statefulSet(names ...string) *apps.StatefulSet {
	sts := &apps.StatefulSet{}
	for _, name := range names {
		sts.Spec.Template.Spec.Containers = append(sts.Spec.Template.Spec.Containers, core.Container{Name: name})
	}
	return sts
}
