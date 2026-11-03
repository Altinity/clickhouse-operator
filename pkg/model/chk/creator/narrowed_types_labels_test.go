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

// External test package (creator_test) so it can import managers to build a real creator;
// managers imports creator, so an in-package test would form an import cycle.
package creator_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"

	chk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chkConfig "github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	chkMacro "github.com/altinity/clickhouse-operator/pkg/model/chk/macro"
	chkNamer "github.com/altinity/clickhouse-operator/pkg/model/chk/namer"
	chkNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chk/normalizer"
	chkLabeler "github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// buildKeeperCreator mirrors worker.buildCreator (pkg/controller/chk/worker.go:111) so the objects
// under test are the ones the controller actually ships.
func buildKeeperCreator(t *testing.T, cr *chk.ClickHouseKeeperInstallation) *commonCreator.Creator {
	t.Helper()
	return commonCreator.NewCreator(
		cr,
		managers.NewConfigFilesGenerator(managers.FilesGeneratorTypeKeeper, cr, &chkConfig.GeneratorOptions{
			Settings: cr.GetSpecT().GetConfiguration().GetSettings(),
			Files:    cr.GetSpecT().GetConfiguration().GetFiles(),
		}),
		managers.NewContainerManager(managers.ContainerManagerTypeKeeper),
		managers.NewTagManager(managers.TagManagerTypeKeeper, cr),
		managers.NewProbeManager(managers.ProbeManagerTypeKeeper),
		managers.NewServiceManager(managers.ServiceManagerTypeKeeper),
		managers.NewVolumeManager(managers.VolumeManagerTypeKeeper),
		managers.NewConfigMapManager(managers.ConfigMapManagerTypeKeeper),
		managers.NewNameManager(managers.NameManagerTypeKeeper),
		managers.NewOwnerReferencesManager(managers.OwnerReferencesManagerTypeKeeper),
		chkNamer.New(),
		chkMacro.New(),
		chkLabeler.New(cr),
	)
}

func normalizedCHK(t *testing.T) *chk.ClickHouseKeeperInstallation {
	t.Helper()
	src := chk.NewClickHouseKeeperInstallation("kpr", "ns")
	src.Spec.Configuration = &chk.Configuration{Clusters: []*chk.Cluster{{Name: "keeper"}}}
	// A volumeClaimTemplate is what makes the StatefulSet controller create PVCs, and those PVCs
	// are the read whose cache miss force-recreates the StatefulSet - so the fixture must have one.
	src.Spec.Defaults = &chi.Defaults{
		Templates: &chi.TemplatesList{VolumeClaimTemplate: "data-volume"},
	}
	src.Spec.Templates = &chi.Templates{
		VolumeClaimTemplates: []chi.VolumeClaimTemplate{{
			Name: "data-volume",
			Spec: core.PersistentVolumeClaimSpec{
				AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteOnce},
			},
		}},
	}
	cr, err := chkNormalizer.New().CreateTemplated(src, commonNormalizer.NewOptions[chk.ClickHouseKeeperInstallation]())
	require.NoError(t, err)
	require.NotNil(t, cr)
	return cr
}

// TestNarrowedTypesCarryTheCacheSelector is the invariant the Keeper manager's narrowed cache
// rests on (see newKeeperCacheOptions in cmd/operator/app/thread_keeper.go).
//
// That cache only holds objects matching clickhouse-keeper.altinity.com/app=chop. A cached read
// for anything else comes back IsNotFound, which several reconcile paths act on - the PVC path
// most destructively, by force-recreating the StatefulSet. So every object type the operator
// creates AND the cache narrows must carry the label. If a creator ever stops emitting it, the
// operator goes blind to its own objects with no error anywhere; this test is the tripwire.
func TestNarrowedTypesCarryTheCacheSelector(t *testing.T) {
	cr := normalizedCHK(t)
	creator := buildKeeperCreator(t, cr)
	selector := k8sLabels.SelectorFromSet(chkLabeler.New(nil).CHOPGeneratedSelector())

	var host *chi.Host
	cr.WalkHosts(func(h *chi.Host) error {
		if host == nil {
			host = h
		}
		return nil
	})
	require.NotNil(t, host, "fixture must yield a host")

	matches := func(t *testing.T, what string, m meta.Object) {
		t.Helper()
		require.True(t, selector.Matches(k8sLabels.Set(m.GetLabels())),
			"%s does not match the cache selector %q - the narrowed cache would never hold it, "+
				"and reads would return IsNotFound; labels were %v",
			what, selector.String(), m.GetLabels())
	}

	sts := creator.CreateStatefulSet(host, false)
	require.NotNil(t, sts)
	matches(t, "StatefulSet", sts)

	// Pods are created by the StatefulSet controller from this template, never by the operator,
	// so the template's labels are the only thing that can make a Keeper Pod visible to the cache.
	matches(t, "Pod template", &meta.ObjectMeta{Labels: sts.Spec.Template.Labels})

	// PVCs likewise: the StatefulSet controller stamps volumeClaimTemplates metadata onto the
	// PVCs it creates. This is the read whose miss force-recreates the StatefulSet.
	require.NotEmpty(t, sts.Spec.VolumeClaimTemplates, "fixture must exercise a volumeClaimTemplate")
	for i := range sts.Spec.VolumeClaimTemplates {
		matches(t, "volumeClaimTemplate["+sts.Spec.VolumeClaimTemplates[i].Name+"]", &sts.Spec.VolumeClaimTemplates[i])
	}

	crServices := creator.CreateService(interfaces.ServiceCR)
	require.Len(t, crServices, 1, "a creator returning nothing would make these assertions vacuous")
	for _, svc := range crServices {
		matches(t, "CR Service", svc)
	}

	// Two per host since the peer/client split: both must be visible to the cache.
	hostServices := creator.CreateService(interfaces.ServiceHost, host)
	require.Len(t, hostServices, 2, "a creator returning nothing would make these assertions vacuous")
	for _, svc := range hostServices {
		matches(t, "host Service "+svc.Name, svc)
	}

	matches(t, "common ConfigMap", creator.CreateConfigMap(interfaces.ConfigMapCommon, chkConfig.NewFilesGeneratorOptions()))
	matches(t, "common-users ConfigMap", creator.CreateConfigMap(interfaces.ConfigMapCommonUsers))
	matches(t, "host ConfigMap", creator.CreateConfigMap(interfaces.ConfigMapHost, host))

	// Cluster scope is a separate label path (_labelPDB / _labelSecret go through getClusterScope,
	// not the host or CR scope exercised above), and the PDB is a narrowed type - so a regression
	// there would hide it from the cache with nothing else failing.
	var cluster chi.ICluster
	cr.WalkClusters(func(c chi.ICluster) error {
		if cluster == nil {
			cluster = c
		}
		return nil
	})
	require.NotNil(t, cluster, "fixture must yield a cluster")
	matches(t, "PodDisruptionBudget", creator.CreatePodDisruptionBudget(cluster))
}
