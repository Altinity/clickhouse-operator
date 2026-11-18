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

package normalizer

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

const (
	templateContainer = "clickhouse-pod"
	serverImage       = "clickhouse/clickhouse-server:25.3"
	olderServerImage  = "clickhouse/clickhouse-server:24.8"
	mirroredImage     = "registry.example.com/altinity/clickhouse-server:24.8"
	customServerImage = "registry.example.com/platform/clickhouse-custom:25.3"
	backupImage       = "altinity/clickhouse-backup:2.6"
	// One registry repository tagged per product, so every image has the same base name.
	productRepoServerImage  = "123456789012.dkr.ecr.us-east-1.amazonaws.com/images:clickhouse-25.3"
	productRepoSidecarImage = "123456789012.dkr.ecr.us-east-1.amazonaws.com/images:fluent-bit-2.1"
)

// normalizeWithPodTemplate normalizes an installation whose hosts all run sharedPodTemplate, built
// from the installation's own pod template, if given, and the given templates' pod templates of that
// name.
func normalizeWithPodTemplate(t *testing.T, own *chi.PodTemplate, fromTemplates ...chi.PodTemplate) *chi.ClickHouseInstallation {
	t.Helper()
	cr := &chi.ClickHouseInstallation{}
	cr.Name = "chi-pod-template"
	cr.Namespace = precedenceNamespace
	for i := range fromTemplates {
		tpl := newCHIT(fmt.Sprintf("chit-pod-template-%d", i), "")
		tpl.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{fromTemplates[i]}}
		registerCHIT(t, tpl)
		cr.Spec.UseTemplates = append(cr.Spec.UseTemplates, &chi.TemplateRef{Name: tpl.Name})
	}
	cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: sharedPodTemplate}}
	if own != nil {
		cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{*own}}
	}
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{{
		Name:   "c",
		Layout: &chi.ChiClusterLayout{ShardsCount: 1, ReplicasCount: 2},
	}}}

	got, err := New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
	require.NoError(t, err)
	return got
}

// ownClickHouse is the installation's own pod template with just the ClickHouse container.
func ownClickHouse(image string) *chi.PodTemplate {
	pt := podTemplate(sharedPodTemplate, container(config.ClickHouseContainerName, image))
	return &pt
}

func requireInvalidPodTemplate(t *testing.T, got *chi.ClickHouseInstallation, merged bool, details ...string) {
	t.Helper()
	require.Equal(t, chi.StatusAborted, got.EnsureStatus().GetStatus())
	errs := got.EnsureStatus().GetErrors()
	require.Len(t, errs, 1, "one abort per installation, not one per host")
	require.True(t, strings.HasPrefix(errs[0], "["+chi.StatusReasonInvalidPodTemplate+"] "), errs[0])
	for _, detail := range details {
		require.Contains(t, errs[0], detail)
	}
	if merged {
		require.Contains(t, errs[0], podTemplateContainerPairingHint, "a merged pod template's message must say how to fix it")
	} else {
		require.NotContains(t, errs[0], podTemplateContainerPairingHint, "a pod template from one layer is not a pairing problem")
	}
}

func requireNotAborted(t *testing.T, got *chi.ClickHouseInstallation) {
	t.Helper()
	require.NotEqual(t, chi.StatusAborted, got.EnsureStatus().GetStatus(), got.EnsureStatus().GetErrors())
}

func withCommand(c core.Container, command ...string) core.Container {
	c.Command = command
	return c
}

func withArgs(c core.Container, args ...string) core.Container {
	c.Args = args
	return c
}

// Containers from templates and the installation pair by name. A template written for the old
// positional merge - its ClickHouse container named `clickhouse-pod`, the installation's
// `clickhouse` - leaves a container of its own, which must stop the reconcile before the StatefulSet
// is written rather than deploy a pod that cannot run.
func TestInvalidPodTemplateAbortsReconcile(t *testing.T) {
	t.Run("a template's ClickHouse container named unlike the installation's starts a duplicate server", func(t *testing.T) {
		got := normalizeWithPodTemplate(t, ownClickHouse(serverImage),
			podTemplate(sharedPodTemplate, container(templateContainer, olderServerImage)))
		requireInvalidPodTemplate(t, got, true,
			fmt.Sprintf("two containers, %q and %q", templateContainer, config.ClickHouseContainerName),
			"merged from "+templateLayerPrefix+precedenceNamespace+"/chit-pod-template-0, "+installationLayer)
	})

	t.Run("the duplicate server is recognized across registries and organizations", func(t *testing.T) {
		got := normalizeWithPodTemplate(t, ownClickHouse(serverImage),
			podTemplate(sharedPodTemplate, container(templateContainer, mirroredImage)))
		requireInvalidPodTemplate(t, got, true, "two containers")
	})

	t.Run("the stock server image is a server next to a custom-built one", func(t *testing.T) {
		got := normalizeWithPodTemplate(t, ownClickHouse(customServerImage),
			podTemplate(sharedPodTemplate, container(templateContainer, olderServerImage)))
		requireInvalidPodTemplate(t, got, true, "two containers")
	})

	for name, c := range map[string]core.Container{
		"through the entrypoint":         withCommand(container(templateContainer, olderServerImage), "/entrypoint.sh"),
		"with server flags":              withArgs(container(templateContainer, olderServerImage), "--config-file=/etc/clickhouse-server/config.xml"),
		"through the server binary":      withCommand(container(templateContainer, olderServerImage), "clickhouse-server", "--config-file=/etc/clickhouse-server/config.xml"),
		"through the multi-call binary":  withCommand(container(templateContainer, olderServerImage), "/usr/bin/clickhouse", "server"),
		"with the entrypoint and a flag": withArgs(withCommand(container(templateContainer, olderServerImage), "/entrypoint.sh"), "--"),
	} {
		t.Run("a template's container that starts the server "+name, func(t *testing.T) {
			got := normalizeWithPodTemplate(t, ownClickHouse(serverImage), podTemplate(sharedPodTemplate, c))
			requireInvalidPodTemplate(t, got, true, "two containers")
		})
	}

	t.Run("two templates' ClickHouse containers named differently", func(t *testing.T) {
		got := normalizeWithPodTemplate(t, nil,
			podTemplate(sharedPodTemplate, container(templateContainer, olderServerImage)),
			podTemplate(sharedPodTemplate, container(config.ClickHouseContainerName, serverImage)))
		requireInvalidPodTemplate(t, got, true, "two containers",
			templateLayerPrefix+precedenceNamespace+"/chit-pod-template-0, "+templateLayerPrefix+precedenceNamespace+"/chit-pod-template-1")
	})

	t.Run("a template's container that only adds mounts has no image of its own", func(t *testing.T) {
		mountsOnly := core.Container{Name: templateContainer, VolumeMounts: []core.VolumeMount{{Name: "cache", MountPath: "/cache"}}}
		got := normalizeWithPodTemplate(t, ownClickHouse(serverImage), podTemplate(sharedPodTemplate, mountsOnly))
		requireInvalidPodTemplate(t, got, true, fmt.Sprintf("has container %q without an image", templateContainer))
	})

	t.Run("a container without a name", func(t *testing.T) {
		got := normalizeWithPodTemplate(t, ownClickHouse(serverImage), podTemplate(sharedPodTemplate, container("", olderServerImage)))
		requireInvalidPodTemplate(t, got, true, "has a container without a name")
	})

	t.Run("an init container without an image", func(t *testing.T) {
		own := ownClickHouse(serverImage)
		own.Spec.InitContainers = []core.Container{{Name: "init"}}
		got := normalizeWithPodTemplate(t, own)
		requireInvalidPodTemplate(t, got, false, `has container "init" without an image`)
	})

	t.Run("an init container named like a container", func(t *testing.T) {
		own := ownClickHouse(serverImage)
		own.Spec.InitContainers = []core.Container{container(config.ClickHouseContainerName, serverImage)}
		got := normalizeWithPodTemplate(t, own)
		requireInvalidPodTemplate(t, got, false, `has two containers named "clickhouse"`)
	})
}

func TestValidPodTemplateIsNotRejected(t *testing.T) {
	t.Run("same-named containers merge into one", func(t *testing.T) {
		got := normalizeWithPodTemplate(t, ownClickHouse(serverImage),
			podTemplate(sharedPodTemplate, container(config.ClickHouseContainerName, olderServerImage)))
		requireNotAborted(t, got)
		require.Len(t, findPodTemplate(t, got, sharedPodTemplate).Spec.Containers, 1)
	})

	t.Run("a template's sidecar is a container of its own", func(t *testing.T) {
		got := normalizeWithPodTemplate(t, ownClickHouse(serverImage),
			podTemplate(sharedPodTemplate, container("clickhouse-backup", backupImage)))
		requireNotAborted(t, got)
		require.Len(t, findPodTemplate(t, got, sharedPodTemplate).Spec.Containers, 2)
	})

	t.Run("a template's sidecar running the client from the server image starts no duplicate server", func(t *testing.T) {
		client := withCommand(container("client", serverImage), "clickhouse-client", "--host", "localhost")
		got := normalizeWithPodTemplate(t, ownClickHouse(serverImage), podTemplate(sharedPodTemplate, client))
		requireNotAborted(t, got)
	})

	t.Run("an installation's own pod template runs what its author wrote", func(t *testing.T) {
		own := podTemplate(sharedPodTemplate,
			container(config.ClickHouseContainerName, productRepoServerImage),
			container("fluent-bit", productRepoSidecarImage))
		got := normalizeWithPodTemplate(t, &own)
		requireNotAborted(t, got)
	})

	t.Run("a template applied automatically and listed in useTemplates is one layer", func(t *testing.T) {
		tpl := newCHIT("chit-auto-and-listed", chi.TemplatingPolicyAuto)
		tpl.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{podTemplate(sharedPodTemplate,
			container(config.ClickHouseContainerName, productRepoServerImage),
			container("fluent-bit", productRepoSidecarImage))}}
		registerCHIT(t, tpl)

		cr := &chi.ClickHouseInstallation{}
		cr.Name = "chi-auto-and-listed"
		cr.Namespace = precedenceNamespace
		cr.Spec.UseTemplates = []*chi.TemplateRef{{Name: tpl.Name}}
		cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: sharedPodTemplate}}
		got, err := New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
		require.NoError(t, err)
		require.Equal(t, 2, got.EnsureStatus().GetUsedTemplatesCount(), "the template must have been applied twice")
		requireNotAborted(t, got)
	})

	t.Run("a pod template listed twice in one resource is one layer", func(t *testing.T) {
		own := podTemplate(sharedPodTemplate,
			container(config.ClickHouseContainerName, productRepoServerImage),
			container("fluent-bit", productRepoSidecarImage))
		cr := &chi.ClickHouseInstallation{}
		cr.Name = "chi-listed-twice"
		cr.Namespace = precedenceNamespace
		cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: sharedPodTemplate}}
		cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{own, own}}
		got, err := New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
		require.NoError(t, err)
		requireNotAborted(t, got)
	})

	t.Run("a pod template no host uses is not checked", func(t *testing.T) {
		cr := &chi.ClickHouseInstallation{}
		cr.Name = "chi-unused-pod-template"
		cr.Namespace = precedenceNamespace
		cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{podTemplate("unused", container("", ""))}}
		got, err := New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
		require.NoError(t, err)
		requireNotAborted(t, got)
	})
}
