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
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/macro"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// productionCreator returns the creator the operator builds the objects of the installation with
func productionCreator(inMemory *api.ClickHouseInstallation) *commonCreator.Creator {
	return commonCreator.NewCreator(
		inMemory,
		managers.NewConfigFilesGenerator(managers.FilesGeneratorTypeClickHouse, inMemory, &config.GeneratorOptions{
			Settings: inMemory.GetSpecT().GetConfiguration().GetSettings(),
			Files:    inMemory.GetSpecT().GetConfiguration().GetFiles(),
		}),
		managers.NewContainerManager(managers.ContainerManagerTypeClickHouse),
		managers.NewTagManager(managers.TagManagerTypeClickHouse, inMemory),
		managers.NewProbeManager(managers.ProbeManagerTypeClickHouse),
		managers.NewServiceManager(managers.ServiceManagerTypeClickHouse),
		managers.NewVolumeManager(managers.VolumeManagerTypeClickHouse),
		managers.NewConfigMapManager(managers.ConfigMapManagerTypeClickHouse),
		managers.NewNameManager(managers.NameManagerTypeClickHouse),
		managers.NewOwnerReferencesManager(managers.OwnerReferencesManagerTypeClickHouse),
		namer.New(),
		macro.New(),
		labeler.New(inMemory),
	)
}

// effectiveIdentity returns the user and group a container of the pod runs as: its own, else the pod's
func effectiveIdentity(pod *core.PodSpec, container *core.Container) (user, group *int64) {
	if pod.SecurityContext != nil {
		user, group = pod.SecurityContext.RunAsUser, pod.SecurityContext.RunAsGroup
	}
	if securityContext := container.SecurityContext; securityContext != nil {
		if securityContext.RunAsUser != nil {
			user = securityContext.RunAsUser
		}
		if securityContext.RunAsGroup != nil {
			group = securityContext.RunAsGroup
		}
	}
	return user, group
}

// Every Job runs as the sidecar of the first host of its cluster runs: its image, pulled alike, under its
// identity, with the registry access and service account of its pod.
func TestJobsRunAsTheSidecarRuns(t *testing.T) {
	for name, tc := range map[string]struct {
		pod        *core.PodSecurityContext
		clickHouse *core.SecurityContext
		sidecar    *core.Container
	}{
		"the operator's defaults": {
			clickHouse: &core.SecurityContext{RunAsUser: ptr.To(int64(101))},
		},
		"the pod template's container": {
			clickHouse: &core.SecurityContext{RunAsUser: ptr.To(int64(101))},
			sidecar: &core.Container{
				Image:           "registry.example.com/clickhouse-backup:fips",
				ImagePullPolicy: core.PullAlways,
				SecurityContext: &core.SecurityContext{RunAsUser: ptr.To(int64(2000)), RunAsGroup: ptr.To(int64(2000)), RunAsNonRoot: ptr.To(true)},
			},
		},
		"the container's user over the pod's": {
			pod:     &core.PodSecurityContext{RunAsUser: ptr.To(int64(101)), RunAsGroup: ptr.To(int64(101))},
			sidecar: &core.Container{SecurityContext: &core.SecurityContext{RunAsUser: ptr.To(int64(2000))}},
		},
		"the pod's user alone": {
			pod: &core.PodSecurityContext{RunAsUser: ptr.To(int64(101)), RunAsGroup: ptr.To(int64(101))},
		},
	} {
		t.Run(name, func(t *testing.T) {
			containers := []core.Container{{
				Name: config.ClickHouseContainerName, Image: "clickhouse/clickhouse-server",
				SecurityContext: tc.clickHouse,
			}}
			if tc.sidecar != nil {
				own := *tc.sidecar.DeepCopy()
				own.Name = config.ClickHouseBackupContainerName
				containers = append(containers, own)
			}
			main := layoutCluster("main", 2, 2, enabledBackup())
			chi := newCHI("demo", main)
			chi.Spec.Templates = &api.Templates{PodTemplates: []api.PodTemplate{{Name: "pod", Spec: core.PodSpec{
				SecurityContext:    tc.pod,
				ImagePullSecrets:   []core.LocalObjectReference{{Name: "registry"}},
				ServiceAccountName: "clickhouse",
				Containers:         containers,
			}}}}
			useTemplate(chi, main, "pod")
			inMemory, stored := normalize(t, chi)

			var pod *core.PodSpec
			inMemory.WalkHosts(func(host *api.Host) error {
				if pod == nil {
					pod = &productionCreator(inMemory).CreateStatefulSet(host, false).Spec.Template.Spec
				}
				return nil
			})
			running, ok := k8s.PodSpecContainerGet(pod, config.ClickHouseBackupContainerName)
			require.True(t, ok)

			topology, err := NewTopology(stored, "")
			require.NoError(t, err)
			job := BuildBackupJob(testBackup("b1"), stored, topology).Spec.Template.Spec
			require.Equal(t, running.Image, job.Containers[0].Image)
			require.Equal(t, running.ImagePullPolicy, job.Containers[0].ImagePullPolicy)
			sidecarUser, sidecarGroup := effectiveIdentity(pod, running)
			jobUser, jobGroup := effectiveIdentity(&job, &job.Containers[0])
			require.Equal(t, sidecarUser, jobUser, "user")
			require.Equal(t, sidecarGroup, jobGroup, "group")
			require.NotNil(t, jobUser, "fixture: every case names a user")
			require.Equal(t, pod.ImagePullSecrets, job.ImagePullSecrets)
			require.Equal(t, pod.ServiceAccountName, job.ServiceAccountName)
		})
	}
}
