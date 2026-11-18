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
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

const (
	precedenceNamespace = "ns"
	sidecarContainer    = "sidecar"
	sharedPodTemplate   = "pt"
	sidecarPodTemplate  = "pt-side"
	sharedVCT           = "data"
)

// registerCHIT enlists a template in the operator-wide catalog for the duration of the test.
func registerCHIT(t *testing.T, chit *chi.ClickHouseInstallation) {
	t.Helper()
	chop.Config().AddCHITemplate(chit)
	t.Cleanup(func() { chop.Config().DeleteCHITemplate(chit) })
}

func newCHIT(name, policy string) *chi.ClickHouseInstallation {
	chit := &chi.ClickHouseInstallation{}
	chit.Name = name
	chit.Namespace = precedenceNamespace
	chit.Spec.Templating = &chi.ChiTemplating{Policy: policy}
	return chit
}

func users(kv ...string) *chi.Settings {
	s := chi.NewSettings()
	for i := 0; i+1 < len(kv); i += 2 {
		s.Set(kv[i], chi.NewSettingScalar(kv[i+1]))
	}
	return s
}

func podTemplate(name string, containers ...core.Container) chi.PodTemplate {
	return chi.PodTemplate{Name: name, Spec: core.PodSpec{Containers: containers}}
}

func container(name, image string, env ...core.EnvVar) core.Container {
	return core.Container{Name: name, Image: image, Env: env}
}

func env(name, value string) core.EnvVar {
	return core.EnvVar{Name: name, Value: value}
}

func vct(name, storageClass string) chi.VolumeClaimTemplate {
	return chi.VolumeClaimTemplate{
		Name: name,
		Spec: core.PersistentVolumeClaimSpec{StorageClassName: &storageClass},
	}
}

func findPodTemplate(t *testing.T, cr *chi.ClickHouseInstallation, name string) *chi.PodTemplate {
	t.Helper()
	for i := range cr.Spec.Templates.PodTemplates {
		if cr.Spec.Templates.PodTemplates[i].Name == name {
			return &cr.Spec.Templates.PodTemplates[i]
		}
	}
	require.Failf(t, "pod template missing", "pod template %q not found", name)
	return nil
}

func findContainer(t *testing.T, pt *chi.PodTemplate, name string) *core.Container {
	t.Helper()
	for i := range pt.Spec.Containers {
		if pt.Spec.Containers[i].Name == name {
			return &pt.Spec.Containers[i]
		}
	}
	require.Failf(t, "container missing", "container %q not found in pod template %q", name, pt.Name)
	return nil
}

func findVCT(t *testing.T, cr *chi.ClickHouseInstallation, name string) *chi.VolumeClaimTemplate {
	t.Helper()
	for i := range cr.Spec.Templates.VolumeClaimTemplates {
		if cr.Spec.Templates.VolumeClaimTemplates[i].Name == name {
			return &cr.Spec.Templates.VolumeClaimTemplates[i]
		}
	}
	require.Failf(t, "volume claim template missing", "volume claim template %q not found", name)
	return nil
}

func envMap(c *core.Container) map[string]string {
	m := map[string]string{}
	for _, e := range c.Env {
		m[e.Name] = e.Value
	}
	return m
}

// TestTemplatesPrecedenceCHIWins drives the production normalizer stack (auto CHIT -> explicit CHIT -> CHI)
// and asserts the documented contract: a value set in the CHI wins over the same value set in any CHIT,
// while CHIT-only values are still applied.
func TestTemplatesPrecedenceCHIWins(t *testing.T) {
	// Explicitly requested template
	manual := newCHIT("chit-manual", "")
	manual.Spec.Suspend = types.NewStringBool(true)
	manual.Spec.Templates = &chi.Templates{
		PodTemplates: []chi.PodTemplate{
			podTemplate(sharedPodTemplate, container(config.ClickHouseContainerName, "chit-image", env("A", "chit-A"), env("B", "chit-B"))),
			podTemplate("pt-chit-only", container(config.ClickHouseContainerName, "chit-only-image")),
			podTemplate(sidecarPodTemplate, container(sidecarContainer, "sidecar-image")),
		},
		VolumeClaimTemplates: []chi.VolumeClaimTemplate{vct(sharedVCT, "chit-class")},
	}
	manual.Spec.Templates.PodTemplates[0].Spec.Containers[0].ImagePullPolicy = core.PullAlways
	manual.Spec.Configuration = &chi.Configuration{
		Users: users("default/password_sha256_hex", "chit-hash", "reader/profile", "chit-reader"),
	}
	registerCHIT(t, manual)

	// Cluster-wide auto template, not referenced by the CHI
	auto := newCHIT("chit-auto", chi.TemplatingPolicyAuto)
	auto.Spec.Templates = &chi.Templates{
		PodTemplates: []chi.PodTemplate{
			podTemplate(sharedPodTemplate, container(config.ClickHouseContainerName, "auto-image", env("A", "auto-A"), env("C", "auto-C"))),
		},
	}
	registerCHIT(t, auto)

	// The installation itself
	cr := &chi.ClickHouseInstallation{}
	cr.Name = "chi"
	cr.Namespace = precedenceNamespace
	cr.Spec.UseTemplates = []*chi.TemplateRef{{Name: manual.Name}}
	cr.Spec.Suspend = types.NewStringBool(false)
	cr.Spec.Templates = &chi.Templates{
		PodTemplates: []chi.PodTemplate{
			podTemplate(sharedPodTemplate, container(config.ClickHouseContainerName, "chi-image", env("A", "chi-A"))),
			podTemplate(sidecarPodTemplate, container(config.ClickHouseContainerName, "chi-image")),
		},
		VolumeClaimTemplates: []chi.VolumeClaimTemplate{vct(sharedVCT, "chi-class")},
	}
	cr.Spec.Configuration = &chi.Configuration{
		Users: users("default/password_sha256_hex", "chi-hash"),
		Clusters: []*chi.Cluster{{
			Name:   "c",
			Layout: &chi.ChiClusterLayout{ShardsCount: 1, ReplicasCount: 1},
		}},
	}

	got, err := New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
	require.NoError(t, err)

	t.Run("same-named pod template: CHI image wins", func(t *testing.T) {
		c := findContainer(t, findPodTemplate(t, got, sharedPodTemplate), config.ClickHouseContainerName)
		require.Equal(t, "chi-image", c.Image)
	})

	t.Run("CHIT-only values still apply", func(t *testing.T) {
		c := findContainer(t, findPodTemplate(t, got, sharedPodTemplate), config.ClickHouseContainerName)
		require.Equal(t, core.PullAlways, c.ImagePullPolicy, "CHIT-only container field must survive")
		findPodTemplate(t, got, "pt-chit-only")
		require.Equal(t, "chit-reader", got.Spec.Configuration.Users.Get("reader/profile").String())
	})

	t.Run("env: CHI wins per name, CHIT-only env kept, no duplicates", func(t *testing.T) {
		c := findContainer(t, findPodTemplate(t, got, sharedPodTemplate), config.ClickHouseContainerName)
		require.Equal(t, map[string]string{"A": "chi-A", "B": "chit-B", "C": "auto-C"}, envMap(c))
		require.Len(t, c.Env, 3)
	})

	t.Run("sidecar in CHIT does not swallow clickhouse container from CHI", func(t *testing.T) {
		pt := findPodTemplate(t, got, sidecarPodTemplate)
		require.Equal(t, "chi-image", findContainer(t, pt, config.ClickHouseContainerName).Image)
		require.Equal(t, "sidecar-image", findContainer(t, pt, sidecarContainer).Image)
		require.Len(t, pt.Spec.Containers, 2)
	})

	t.Run("users: CHI password wins", func(t *testing.T) {
		require.Equal(t, "chi-hash", got.Spec.Configuration.Users.Get("default/password_sha256_hex").String())
	})

	t.Run("volume claim template: CHI storage class wins", func(t *testing.T) {
		require.Equal(t, "chi-class", *findVCT(t, got, sharedVCT).Spec.StorageClassName)
	})

	t.Run("suspend: CHI wins", func(t *testing.T) {
		require.False(t, got.Spec.Suspend.Value(), "CHI suspend=false must beat CHIT suspend=true")
	})

	t.Run("auto template applied but CHI still wins", func(t *testing.T) {
		require.Len(t, got.Status.UsedTemplates, 2)
		require.Equal(t, auto.Name, got.Status.UsedTemplates[0].Name, "auto template goes first")
		require.Equal(t, manual.Name, got.Status.UsedTemplates[1].Name, "explicit template goes after auto")
	})
}

// TestTemplatesPrecedenceCHIWinsSpecAndMetadata extends the contract to the fields outside the
// template sections: the installation's metadata, operational controls, template selection and
// zookeeper config beat a template's, while what only the installation defines survives.
func TestTemplatesPrecedenceCHIWinsSpecAndMetadata(t *testing.T) {
	tpl := newCHIT("chit-spec", "")
	tpl.Labels = map[string]string{"owner": "tpl"}
	tpl.Spec.Suspend = types.NewStringBool(true)
	tpl.Spec.Stop = types.NewStringBool(true)
	tpl.Spec.Troubleshoot = types.NewStringBool(true)
	tpl.Spec.TaskID = (*types.Id)(types.NewString("tpl-task"))
	// explicitly empty, which counts as set: it must not swallow the installation's restart
	tpl.Spec.Restart = types.NewString("")
	tpl.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: sharedPodTemplate}}
	tpl.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{
		podTemplate(sharedPodTemplate, container(config.ClickHouseContainerName, "chit-image")),
	}}
	tpl.Spec.Configuration = &chi.Configuration{
		Users:     users("default/password_sha256_hex", "chit-hash"),
		Zookeeper: &chi.ZookeeperConfig{Root: "/tpl"},
	}
	registerCHIT(t, tpl)

	cr := &chi.ClickHouseInstallation{}
	cr.Name = "chi-spec"
	cr.Namespace = precedenceNamespace
	cr.Labels = map[string]string{"owner": "chi"}
	cr.Spec.UseTemplates = []*chi.TemplateRef{{Name: tpl.Name}}
	cr.Spec.Suspend = types.NewStringBool(false)
	cr.Spec.Stop = types.NewStringBool(false)
	cr.Spec.Troubleshoot = types.NewStringBool(false)
	cr.Spec.TaskID = (*types.Id)(types.NewString("cr-task"))
	cr.Spec.Restart = types.NewString(chi.RestartRollingUpdate)
	cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: "pt-chi-only"}}
	cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{
		podTemplate(sharedPodTemplate, container(config.ClickHouseContainerName, "chi-image")),
		podTemplate("pt-chi-only", container(config.ClickHouseContainerName, "chi-only-image")),
	}}
	cr.Spec.Configuration = &chi.Configuration{
		Users:     users("default/password_sha256_hex", "chi-hash"),
		Zookeeper: &chi.ZookeeperConfig{Root: "/cr"},
		Clusters: []*chi.Cluster{{
			Name:   "c",
			Layout: &chi.ChiClusterLayout{ShardsCount: 1, ReplicasCount: 1},
		}},
	}

	got, err := New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
	require.NoError(t, err)

	require.Equal(t, "chi-image", findContainer(t, findPodTemplate(t, got, sharedPodTemplate), config.ClickHouseContainerName).Image)
	require.Equal(t, "chi-only-image", findContainer(t, findPodTemplate(t, got, "pt-chi-only"), config.ClickHouseContainerName).Image)
	require.Equal(t, "chi-hash", got.Spec.Configuration.Users.Get("default/password_sha256_hex").String())
	require.False(t, got.Spec.Suspend.Value())
	require.False(t, got.Spec.Stop.Value())
	require.False(t, got.Spec.Troubleshoot.Value())
	require.Equal(t, "cr-task", got.Spec.TaskID.String())
	require.Equal(t, chi.RestartRollingUpdate, got.Spec.Restart.Value())
	require.Equal(t, "pt-chi-only", got.Spec.GetDefaults().Templates.PodTemplate)
	require.Equal(t, "chi", got.Labels["owner"])
	require.Equal(t, "/cr", got.Spec.Configuration.Zookeeper.Root)
}

// A cluster that names no nodes inherits the installation-level zookeeper config, and inherits it
// the way it always has: the installation's root, identity and timeouts replace the cluster's own.
// Such a cluster's replicated tables were created under that root, so honouring the cluster's own
// root now would move their ZooKeeper paths on upgrade. Its own use_compression is kept, as before.
func TestClusterZookeeperInheritsCHILevel(t *testing.T) {
	cr := &chi.ClickHouseInstallation{}
	cr.Name = "chi-zk"
	cr.Namespace = precedenceNamespace
	cr.Spec.Configuration = &chi.Configuration{
		Zookeeper: &chi.ZookeeperConfig{
			Nodes:              chi.ZookeeperNodes{{Host: "zk-chi"}},
			SessionTimeoutMs:   100,
			OperationTimeoutMs: 1000,
			Root:               "/chi",
			Identity:           "chi:secret",
			UseCompression:     types.NewStringBool(true),
		},
		Clusters: []*chi.Cluster{{
			Name:   "c",
			Layout: &chi.ChiClusterLayout{ShardsCount: 1, ReplicasCount: 1},
			Zookeeper: &chi.ZookeeperConfig{
				SessionTimeoutMs: 200,
				Root:             "/cluster",
				Identity:         "cluster:secret",
				UseCompression:   types.NewStringBool(false),
			},
		}},
	}

	got, err := New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
	require.NoError(t, err)

	zk := got.Spec.Configuration.Clusters[0].Zookeeper
	require.Equal(t, "/chi", zk.Root, "a different root would move the cluster's replicated tables")
	require.Equal(t, "chi:secret", zk.Identity, "a different identity would fail ZooKeeper auth")
	require.Equal(t, 100, zk.SessionTimeoutMs)
	require.Equal(t, 1000, zk.OperationTimeoutMs)
	require.Equal(t, "zk-chi", zk.Nodes[0].Host, "nodes are inherited")
	require.False(t, zk.UseCompression.Value(), "the cluster's own use_compression is kept")
}

// The operator's own configuration rules are the bottom layer, beneath every template, like any
// other default: they fill what neither a template nor the installation sets, and lose to either
// on a key both set. The operator user's grants are written for the user placeholder, which is
// expanded after the merge, so a grant spelled with the user's name is overwritten rather than
// overriding it. Merged in whole as that layer, the rules must not cost the installation its
// clusters or its runtime attributes, which the layers above them bring.
func TestOperatorRulesAreTheBottomLayer(t *testing.T) {
	const (
		secretsSetting                       = "display_secrets_in_show_and_select"
		grantInstallationSpellsByName        = "access_management"
		grantInstallationSpellsByPlaceholder = "show_named_collections"
		grantOnlyInRules                     = "show_named_collections_secrets"
	)
	byPlaceholder := func(grant string) string { return clickhouseOperatorUserMacro + "/" + grant }
	byUserName := func(grant string) string { return chop.Config().ClickHouse.Access.Username + "/" + grant }

	tpl := newCHIT("chit-rules", "")
	tpl.Spec.Configuration = &chi.Configuration{Settings: users(secretsSetting, "0")}
	registerCHIT(t, tpl)

	rules := &chi.ClickHouseInstallation{Spec: chi.ChiSpec{Configuration: &chi.Configuration{
		Settings: users(secretsSetting, "1"),
		Users:    users(byPlaceholder(grantInstallationSpellsByName), "1", byPlaceholder(grantInstallationSpellsByPlaceholder), "1", byPlaceholder(grantOnlyInRules), "1"),
	}}}

	cr := &chi.ClickHouseInstallation{}
	cr.Name = "chi-rules"
	cr.Namespace = precedenceNamespace
	cr.Spec.UseTemplates = []*chi.TemplateRef{{Name: tpl.Name}}
	cr.Spec.Configuration = &chi.Configuration{
		Users: users(byUserName(grantInstallationSpellsByName), "0", byPlaceholder(grantInstallationSpellsByPlaceholder), "0"),
		Clusters: []*chi.Cluster{{
			Name:   "mycluster",
			Layout: &chi.ChiClusterLayout{ShardsCount: 2, ReplicasCount: 2},
		}},
	}
	cr.EnsureRuntime().GetAttributes().SetSkipOwnerRef(true)

	opts := commonNormalizer.NewOptions[chi.ClickHouseInstallation]()
	opts.Templates = []*chi.ClickHouseInstallation{rules}
	got, err := New(nil).CreateTemplated(cr, opts)
	require.NoError(t, err)

	gotUsers := got.Spec.Configuration.Users
	require.Equal(t, "0", got.Spec.Configuration.Settings.Get(secretsSetting).String(), "a template wins over the rules")
	require.Equal(t, "0", gotUsers.Get(byUserName(grantInstallationSpellsByPlaceholder)).String(), "the installation wins over the rules on the same key")
	require.Equal(t, "1", gotUsers.Get(byUserName(grantInstallationSpellsByName)).String(),
		"a grant spelled with the user's name is overwritten when the rules' placeholder is expanded")
	require.Equal(t, "1", gotUsers.Get(byUserName(grantOnlyInRules)).String(), "the rules fill what nobody else sets")

	require.Len(t, got.Spec.Configuration.Clusters, 1)
	cluster := got.Spec.Configuration.Clusters[0]
	require.Equal(t, "mycluster", cluster.Name, "the installation's clusters must survive")
	require.Equal(t, 4, cluster.HostsCount(), "every host of the installation must survive")
	require.True(t, got.EnsureRuntime().GetAttributes().GetSkipOwnerRef(),
		"the installation's runtime attributes must survive")
}
