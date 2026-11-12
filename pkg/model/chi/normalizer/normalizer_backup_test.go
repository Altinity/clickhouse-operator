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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/subst"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

const (
	backupCRName         = "chi-backup"
	backupSecretName     = backupCRName + "-auto-backup-secret"
	backupSecretPassword = "GENERATEDBYTHEOPERATOR2345"
)

// backupSecretGetter serves the backup Secret of the test installation the way the operator created it
func backupSecretGetter(t *testing.T) subst.SecretGetter {
	return func(namespace, name string) (*core.Secret, error) {
		if (namespace != precedenceNamespace) || (name != backupSecretName) {
			t.Fatalf("unexpected Secret read: %s/%s", namespace, name)
		}
		return &core.Secret{Data: map[string][]byte{
			config.ClickHouseBackupSecretKeyClickHouseUsername: []byte(testBackupUsername),
			config.ClickHouseBackupSecretKeyClickHousePassword: []byte(backupSecretPassword),
		}}, nil
	}
}

// missingSecretGetter answers as the API does before the operator has created the backup Secret
func missingSecretGetter(namespace, name string) (*core.Secret, error) {
	return nil, apiErrors.NewNotFound(core.Resource("secrets"), name)
}

func normalizeBackupCR(t *testing.T, cr *chi.ClickHouseInstallation, secretGet subst.SecretGetter) *chi.ClickHouseInstallation {
	t.Helper()
	cr.Name = backupCRName
	cr.Namespace = precedenceNamespace
	got, err := New(secretGet).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
	require.NoError(t, err)
	return got
}

// backupStorageSecret is the storage Secret enabledBackup names, without which backup stays off
const backupStorageSecret = "s3-credentials"

// configSecretRef references the configuration Secret of the given name
func configSecretRef(secret string) *core.LocalObjectReference {
	return &core.LocalObjectReference{Name: secret}
}

// enabledBackup enables backup with a storage Secret
func enabledBackup() *chi.ClusterBackup {
	return &chi.ClusterBackup{
		Enabled:         types.NewStringBool(true),
		ConfigSecretRef: configSecretRef(backupStorageSecret),
	}
}

// enabledOn enables backup with a storage Secret and the sidecar built on the clickhouse-backup container of the
// pod template named
func enabledOn(podTemplate string) *chi.ClusterBackup {
	return &chi.ClusterBackup{
		Enabled:         types.NewStringBool(true),
		PodTemplate:     types.NewString(podTemplate),
		ConfigSecretRef: configSecretRef(backupStorageSecret),
	}
}

func backupCluster(name string, backup *chi.ClusterBackup) *chi.Cluster {
	return &chi.Cluster{
		Name:   name,
		Layout: &chi.ChiClusterLayout{ShardsCount: 1, ReplicasCount: 1},
		Backup: backup,
	}
}

func findCluster(t *testing.T, cr *chi.ClickHouseInstallation, name string) *chi.Cluster {
	t.Helper()
	cluster, ok := cr.FindCluster(name).(*chi.Cluster)
	require.True(t, ok && (cluster != nil), "cluster %q not found", name)
	return cluster
}

func backupUser(field string) string {
	return testBackupUsername + "/" + field
}

// requireBackupUserFromSecret checks the backup user authenticates with the password of the backup Secret
func requireBackupUserFromSecret(t *testing.T, cr *chi.ClickHouseInstallation) {
	t.Helper()
	users := cr.Spec.Configuration.Users
	hash := sha256.Sum256([]byte(backupSecretPassword))
	require.Equal(t, hex.EncodeToString(hash[:]), users.Get(backupUser(userFieldPasswordSHA256Hex)).String())
	require.False(t, users.Has(backupUser(userFieldPassword)), "the plaintext password must never reach the users config")
	require.ElementsMatch(t, config.ClickHouseBackupUserNetworksIP, users.Get(backupUser(userFieldNetworksIP)).AsVectorOfStrings())
	require.False(t, users.Has(backupUser(userFieldNetworksHostRegexp)), "a host regexp would let pods of the installation in")
}

// requireNoBackupUser checks the installation has no trace of the backup user
func requireNoBackupUser(t *testing.T, cr *chi.ClickHouseInstallation) {
	t.Helper()
	cr.Spec.Configuration.Users.WalkKeys(func(key string, _ *chi.Setting) {
		require.NotContains(t, key, testBackupUsername+"/")
	})
}

// requireNoBackupEnvVar checks no ClickHouse container is given anything from the backup Secret. Users are
// common for all clusters, so such an env var would restart every host when one cluster enables backup.
func requireNoBackupEnvVar(t *testing.T, cr *chi.ClickHouseInstallation) {
	t.Helper()
	for _, envVar := range cr.GetRuntime().GetAttributes().GetAdditionalEnvVars() {
		if (envVar.ValueFrom != nil) && (envVar.ValueFrom.SecretKeyRef != nil) {
			require.NotEqual(t, backupSecretName, envVar.ValueFrom.SecretKeyRef.Name, "env var %s", envVar.Name)
		}
	}
}

// Clusters inherit every field of .spec.backup they leave empty, so a cluster can opt out with
// `enabled: no` and still shares the installation's other settings.
func TestBackupClusterInheritance(t *testing.T) {
	cr := &chi.ClickHouseInstallation{}
	cr.Spec.Backup = &chi.ClusterBackup{
		Enabled:         types.NewStringBool(true),
		ConfigSecretRef: configSecretRef(backupStorageSecret),
	}
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{
		backupCluster("inherits", nil),
		backupCluster("opts-out", &chi.ClusterBackup{Enabled: types.NewStringBool(false)}),
	}}

	got := normalizeBackupCR(t, cr, backupSecretGetter(t))
	requireNotAborted(t, got)

	inherits := findCluster(t, got, "inherits").GetBackup()
	require.True(t, inherits.IsEnabled())
	require.Equal(t, backupStorageSecret, inherits.GetConfigSecretName())

	optsOut := findCluster(t, got, "opts-out").GetBackup()
	require.False(t, optsOut.IsEnabled(), "a cluster-level `enabled: no` wins over the installation-level `yes`")
	require.Equal(t, backupStorageSecret, optsOut.GetConfigSecretName())
}

// .spec.backup is part of the spec, so a template can switch backup on for every installation using it.
func TestBackupFromTemplate(t *testing.T) {
	tpl := newCHIT("chit-backup", "")
	tpl.Spec.Backup = &chi.ClusterBackup{
		Enabled:         types.NewStringBool(true),
		ConfigSecretRef: configSecretRef(backupStorageSecret),
	}
	registerCHIT(t, tpl)

	cr := &chi.ClickHouseInstallation{}
	cr.Spec.UseTemplates = []*chi.TemplateRef{{Name: tpl.Name}}
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{
		backupCluster("c", nil),
	}}

	got := normalizeBackupCR(t, cr, backupSecretGetter(t))
	backup := findCluster(t, got, "c").GetBackup()
	require.True(t, backup.IsEnabled())
	require.Equal(t, backupStorageSecret, backup.GetConfigSecretName())
	requireBackupUserFromSecret(t, got)
}

// A cluster takes the installation's configuration Secret unless it names one of its own. An explicit name ""
// leaves the cluster without one rather than inheriting the installation's.
func TestBackupConfigSecretInheritance(t *testing.T) {
	cr := &chi.ClickHouseInstallation{}
	cr.Spec.Backup = &chi.ClusterBackup{
		Enabled:         types.NewStringBool(true),
		ConfigSecretRef: configSecretRef(backupStorageSecret),
	}
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{
		backupCluster("inherits", nil),
		backupCluster("own", &chi.ClusterBackup{ConfigSecretRef: configSecretRef("s3-cold")}),
		backupCluster("opts-out", &chi.ClusterBackup{ConfigSecretRef: configSecretRef("")}),
	}}

	got := normalizeBackupCR(t, cr, backupSecretGetter(t))
	requireNotAborted(t, got)
	for cluster, want := range map[string]string{
		"inherits": backupStorageSecret,
		"own":      "s3-cold",
		"opts-out": "",
	} {
		require.Equal(t, want, findCluster(t, got, cluster).GetBackup().GetConfigSecretName(), cluster)
	}
	require.Equal(t, backupStorageSecret, got.Spec.Backup.GetConfigSecretName(), "the installation keeps its own Secret")
}

// An installation's own configuration Secret wins over the one of a template it uses: the template applies to many
// installations, each keeping its backups where it says.
func TestBackupConfigSecretOverATemplate(t *testing.T) {
	tpl := newCHIT("chit-storage", "")
	tpl.Spec.Backup = &chi.ClusterBackup{
		Enabled:         types.NewStringBool(true),
		ConfigSecretRef: configSecretRef("s3-shared"),
	}
	registerCHIT(t, tpl)

	for name, tc := range map[string]struct {
		backup *chi.ClusterBackup
		secret string
	}{
		"an installation of no Secret": {secret: "s3-shared"},
		"an installation of a Secret of its own": {
			backup: &chi.ClusterBackup{ConfigSecretRef: configSecretRef("s3-own")},
			secret: "s3-own",
		},
	} {
		t.Run(name, func(t *testing.T) {
			cr := &chi.ClickHouseInstallation{}
			cr.Spec.UseTemplates = []*chi.TemplateRef{{Name: tpl.Name}}
			cr.Spec.Backup = tc.backup
			cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{backupCluster("c", nil)}}

			got := normalizeBackupCR(t, cr, backupSecretGetter(t))
			requireNotAborted(t, got)
			require.Equal(t, tc.secret, findCluster(t, got, "c").GetBackup().GetConfigSecretName())
			require.Equal(t, "s3-shared", tpl.Spec.Backup.GetConfigSecretName(), "the template keeps its Secret")
		})
	}
}

// A recovery is inherited by every cluster like the other backup settings, and an explicit "" on a
// cluster leaves it out of the recovery: a cluster the backup does not hold, added to the installation
// later, must not keep the others from being recovered.
func TestBackupRecoveryInheritance(t *testing.T) {
	cr := &chi.ClickHouseInstallation{}
	cr.Spec.Backup = &chi.ClusterBackup{
		Enabled:  types.NewStringBool(true),
		Recovery: &chi.BackupRecovery{BackupName: types.NewString("nightly-20261004-020000")},
	}
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{
		backupCluster("inherits", nil),
		backupCluster("own", &chi.ClusterBackup{Recovery: &chi.BackupRecovery{BackupName: types.NewString("cold-20261001-030000")}}),
		backupCluster("opts-out", &chi.ClusterBackup{Recovery: &chi.BackupRecovery{BackupName: types.NewString("")}}),
	}}

	got := normalizeBackupCR(t, cr, backupSecretGetter(t))
	requireNotAborted(t, got)
	require.Equal(t, "nightly-20261004-020000", findCluster(t, got, "inherits").GetBackup().GetRecovery().GetBackupName().Value())
	require.Equal(t, "cold-20261001-030000", findCluster(t, got, "own").GetBackup().GetRecovery().GetBackupName().Value(),
		"a cluster-level recovery wins over the installation-level one")
	optsOut := findCluster(t, got, "opts-out").GetBackup().GetRecovery().GetBackupName()
	require.True(t, optsOut.HasValue(), "an explicit empty name is kept, so it is not inherited over")
	require.Empty(t, optsOut.Value())
}

// A template applies to many installations: a recovery it named would restore the same backup into
// each of them. A recovery is therefore only ever taken from the installation itself, whatever the
// templates say, at the installation level or on a cluster.
func TestBackupRecoveryNeverComesFromATemplate(t *testing.T) {
	tpl := newCHIT("chit-recovery", "")
	tpl.Spec.Backup = enabledBackup()
	tpl.Spec.Backup.Recovery = &chi.BackupRecovery{BackupName: types.NewString("tenant-a-20261004-020000")}
	tpl.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{
		backupCluster("c", &chi.ClusterBackup{Recovery: &chi.BackupRecovery{BackupName: types.NewString("tenant-a-c-20261004-020000")}}),
	}}
	registerCHIT(t, tpl)

	for name, configuration := range map[string]*chi.Configuration{
		"a cluster of the installation":  {Clusters: []*chi.Cluster{backupCluster("c", nil)}},
		"a cluster the template defines": nil,
	} {
		t.Run(name, func(t *testing.T) {
			cr := &chi.ClickHouseInstallation{}
			cr.Spec.UseTemplates = []*chi.TemplateRef{{Name: tpl.Name}}
			cr.Spec.Configuration = configuration

			got := normalizeBackupCR(t, cr, backupSecretGetter(t))
			require.True(t, findCluster(t, got, "c").GetBackup().IsEnabled(), "the template's other backup settings apply")
			require.Nil(t, findCluster(t, got, "c").GetBackup().GetRecovery())
			require.Nil(t, got.Spec.Backup.GetRecovery())
			require.False(t, got.RequestsBackupRecovery())
		})
	}

	t.Run("an installation with a recovery of its own", func(t *testing.T) {
		cr := &chi.ClickHouseInstallation{}
		cr.Spec.UseTemplates = []*chi.TemplateRef{{Name: tpl.Name}}
		cr.Spec.Backup = &chi.ClusterBackup{Recovery: &chi.BackupRecovery{BackupName: types.NewString("own-20261004-020000")}}

		got := normalizeBackupCR(t, cr, backupSecretGetter(t))
		require.Equal(t, "own-20261004-020000", findCluster(t, got, "c").GetBackup().GetRecovery().GetBackupName().Value(),
			"a cluster the template defines inherits the installation's own recovery, never the template's")
	})
}

// A template is shared by every installation it applies to: the recovery an installation gives the
// clusters it takes from a template goes to copies of its own, so neither the template nor another
// installation normalized later sees it, and dropping the template's own recovery leaves the template
// as it is.
func TestBackupRecoveryLeavesTheTemplateAlone(t *testing.T) {
	const templateRecovery = "tenant-a-c-20261004-020000"
	tpl := newCHIT("chit-shared-recovery", "")
	tpl.Spec.Backup = enabledBackup()
	tpl.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{
		backupCluster("c", &chi.ClusterBackup{Recovery: &chi.BackupRecovery{BackupName: types.NewString(templateRecovery)}}),
	}}
	registerCHIT(t, tpl)

	for i, recovery := range []string{"own-20261004-020000", ""} {
		cr := &chi.ClickHouseInstallation{}
		cr.Spec.UseTemplates = []*chi.TemplateRef{{Name: tpl.Name}}
		if recovery != "" {
			cr.Spec.Backup = &chi.ClusterBackup{Recovery: &chi.BackupRecovery{BackupName: types.NewString(recovery)}}
		}
		got := normalizeBackupCR(t, cr, backupSecretGetter(t))
		require.Equal(t, recovery, findCluster(t, got, "c").GetBackup().GetRecovery().GetBackupName().Value(),
			"installation %d recovers from its own backup, if any", i)
		require.Equal(t, templateRecovery, tpl.Spec.Configuration.Clusters[0].GetBackup().GetRecovery().GetBackupName().Value(),
			"the template is left as it is")
		require.Zero(t, tpl.Spec.Configuration.Clusters[0].Runtime, "the template's cluster is not the one normalized")
	}
}

// An installation that never mentions backup must normalize exactly as before: no backup block,
// no backup user, no Secret read - otherwise every existing installation would change on upgrade.
func TestBackupDisabledAddsNothing(t *testing.T) {
	cr := &chi.ClickHouseInstallation{}
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{
		backupCluster("c", nil),
	}}

	got := normalizeBackupCR(t, cr, func(namespace, name string) (*core.Secret, error) {
		t.Fatalf("an installation without backup read Secret %s/%s", namespace, name)
		return nil, nil
	})
	require.Nil(t, findCluster(t, got, "c").GetBackup())
	requireNoBackupUser(t, got)
	requireNoBackupEnvVar(t, got)
}

// The backup user exists once any cluster has backup on - users are common for the whole installation -
// authenticating with the password of the backup Secret and reachable from the pod only. The password goes
// into the users config as a hash, not into the ClickHouse containers as an env var.
func TestBackupUserInjected(t *testing.T) {
	cr := &chi.ClickHouseInstallation{}
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{
		backupCluster("plain", nil),
		backupCluster("backed-up", enabledBackup()),
	}}

	got := normalizeBackupCR(t, cr, backupSecretGetter(t))
	requireBackupUserFromSecret(t, got)
	requireNoBackupEnvVar(t, got)

	users := got.Spec.Configuration.Users
	require.Equal(t, chop.Config().ClickHouse.Config.User.Default.Profile, users.Get(backupUser(userFieldProfile)).String())
	require.Equal(t, chop.Config().ClickHouse.Config.User.Default.Quota, users.Get(backupUser(userFieldQuota)).String())
}

// In backup.mode pod the backup user connects from the backup Pods of the shards, over the network: it is let in
// from their DNS names, and from nowhere else besides the pod's loopback.
func TestBackupUserOfPodModeComesFromTheBackupPods(t *testing.T) {
	podMode := enabledBackup()
	podMode.Mode = types.NewString(chi.BackupModePod)
	cr := &chi.ClickHouseInstallation{}
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{
		backupCluster("sidecars", enabledBackup()),
		backupCluster("pods", podMode),
	}}

	got := normalizeBackupCR(t, cr, backupSecretGetter(t))
	users := got.Spec.Configuration.Users
	require.ElementsMatch(t, config.ClickHouseBackupUserNetworksIP, users.Get(backupUser(userFieldNetworksIP)).AsVectorOfStrings())
	expression := users.Get(backupUser(userFieldNetworksHostRegexp)).String()
	require.NotEmpty(t, expression)
	re := regexp.MustCompile(expression)
	require.True(t, re.MatchString("chi-chi-backup-pods-0-bk.chi-backup-pods-backup.ns.svc.cluster.local"))
	for _, other := range []string{
		"chi-chi-backup-pods-0-0.ns.svc.cluster.local",                            // a ClickHouse host
		"chi-chi-backup-pods-0-bk.chi-backup-pods-backup.other.svc.cluster.local", // another namespace
		"chi-other-pods-0-bk.other-pods-backup.ns.svc.cluster.local",              // another installation
		"x.chi-chi-backup-pods-0-bk.chi-backup-pods-backup.ns.svc.cluster.local",  // not anchored
	} {
		require.False(t, re.MatchString(other), other)
	}
}

// Sidecars connect over loopback only: no host regexp.
func TestBackupUserOfSidecarModeHasNoHostRegexp(t *testing.T) {
	cr := &chi.ClickHouseInstallation{}
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{backupCluster("sidecars", enabledBackup())}}
	got := normalizeBackupCR(t, cr, backupSecretGetter(t))
	require.False(t, got.Spec.Configuration.Users.Has(backupUser(userFieldNetworksHostRegexp)))
}

// The operator manages a backup user only when the backup Secret sets both its username and its password, which are
// the user's to set: there is no username of the operator's own. Without them there is no backup user, and a user
// of any name the CR defines stays the CR's own.
func TestBackupUserWithoutCredentials(t *testing.T) {
	for name, tc := range map[string]struct {
		secretGet subst.SecretGetter
	}{
		"Secret not created yet": {secretGet: missingSecretGetter},
		"no Secret getter":       {},
		"Secret lacks the username": {secretGet: func(namespace, name string) (*core.Secret, error) {
			return &core.Secret{Data: map[string][]byte{
				config.ClickHouseBackupSecretKeyClickHouseUsername: []byte(""),
				config.ClickHouseBackupSecretKeyClickHousePassword: []byte(backupSecretPassword),
			}}, nil
		}},
		"Secret lacks the password": {secretGet: func(namespace, name string) (*core.Secret, error) {
			return &core.Secret{Data: map[string][]byte{
				config.ClickHouseBackupSecretKeyClickHouseUsername: []byte(testBackupUsername),
			}}, nil
		}},
	} {
		t.Run(name, func(t *testing.T) {
			cr := &chi.ClickHouseInstallation{}
			cr.Spec.Configuration = &chi.Configuration{
				Users: users(backupUser(userFieldProfile), "backup-profile"),
				Clusters: []*chi.Cluster{
					backupCluster("c", enabledBackup()),
				},
			}

			got := normalizeBackupCR(t, cr, tc.secretGet)
			requireNotAborted(t, got)
			require.True(t, findCluster(t, got, "c").GetBackup().IsEnabled())
			requireNoBackupEnvVar(t, got)
			users := got.Spec.Configuration.Users
			require.False(t, users.Has("/"+userFieldPasswordSHA256Hex), "no user of an empty name")
			// No username and password both, no backup user: a user of that name is the CR's own
			require.Equal(t, "backup-profile", users.Get(backupUser(userFieldProfile)).String())
			require.NotElementsMatch(t, config.ClickHouseBackupUserNetworksIP, users.Get(backupUser(userFieldNetworksIP)).AsVectorOfStrings())
		})
	}
}

// Only a Secret that is not there leaves the backup user pending. Any other error reading it leaves the
// operator unable to tell, and going on without the user would lock every sidecar out, so the CR is aborted.
func TestBackupUserSecretReadError(t *testing.T) {
	cr := &chi.ClickHouseInstallation{}
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{
		backupCluster("c", enabledBackup()),
	}}

	got := normalizeBackupCR(t, cr, func(namespace, name string) (*core.Secret, error) {
		return nil, apiErrors.NewForbidden(core.Resource("secrets"), name, errors.New("RBAC denies get"))
	})
	status := got.EnsureStatus()
	require.Equal(t, chi.StatusAborted, status.GetStatus())
	require.NotEmpty(t, status.GetErrors())
	require.True(t, strings.HasPrefix(status.GetErrors()[0], "["+chi.StatusReasonBackupSecretUnresolved+"] "), status.GetErrors()[0])
	require.Contains(t, status.GetErrors()[0], backupSecretName)
	requireNoBackupUser(t, got)
}

// The operator owns the backup user: whatever the installation says about it besides profile and quota
// is replaced, so the sidecars can always log in and nobody can widen where it is reachable from.
func TestBackupUserOwnedByOperator(t *testing.T) {
	cr := &chi.ClickHouseInstallation{}
	cr.Spec.Configuration = &chi.Configuration{
		Users: users(
			backupUser(userFieldPassword), "chosen-by-user",
			backupUser(userFieldNetworksIP), "::/0",
			backupUser(userFieldNetworksHostRegexp), ".*",
			backupUser("ssl_certificates/common_name"), "backup",
			backupUser(userFieldProfile), "backup-profile",
		),
		Clusters: []*chi.Cluster{
			backupCluster("c", enabledBackup()),
		},
	}

	got := normalizeBackupCR(t, cr, backupSecretGetter(t))
	users := got.Spec.Configuration.Users

	requireBackupUserFromSecret(t, got)
	require.False(t, users.Has(backupUser("ssl_certificates/common_name")), "a second authentication method breaks the users config")
	require.Equal(t, "backup-profile", users.Get(backupUser(userFieldProfile)).String(), "profile stays the user's call")
}

// A container named like the sidecar in the pod template the backup names is a base the operator fills in, so it
// may come without an image - but only for hosts of a backup-enabled cluster, where the operator does fill it in.
func TestBackupBaseContainerWithoutImage(t *testing.T) {
	newCR := func(backup *chi.ClusterBackup) *chi.ClickHouseInstallation {
		cr := &chi.ClickHouseInstallation{}
		cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: sharedPodTemplate}}
		cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{
			podTemplate(sharedPodTemplate,
				container(config.ClickHouseContainerName, serverImage),
				container(config.ClickHouseBackupContainerName, "", env("LOG_LEVEL", "debug")),
			),
		}}
		cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{backupCluster("c", backup)}}
		return cr
	}

	t.Run("backup enabled - the operator provides the image", func(t *testing.T) {
		backup := enabledOn(sharedPodTemplate)
		requireNotAborted(t, normalizeBackupCR(t, newCR(backup), backupSecretGetter(t)))
	})
	t.Run("backup disabled - nothing provides the image", func(t *testing.T) {
		requireInvalidPodTemplate(t, normalizeBackupCR(t, newCR(nil), nil), false,
			"container \""+config.ClickHouseBackupContainerName+"\" without an image")
	})
}

// Under security.images.policy=Required the operator's clickhouse-backup image must be a FIPS build as well:
// the sidecar runs it next to every ClickHouse server of a backup-enabled cluster, and the backup Jobs run it.
func TestBackupFIPSImagePolicy(t *testing.T) {
	const (
		fipsServerImage = "altinity/clickhouse-server:25.3.fips"
		fipsBackupImage = "altinity/clickhouse-backup:2.8.1-fips"
		userBackupImage = "registry.example.com/clickhouse-backup:custom"
	)
	newCR := func(backup *chi.ClusterBackup, containers ...core.Container) *chi.ClickHouseInstallation {
		cr := &chi.ClickHouseInstallation{}
		cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: sharedPodTemplate}}
		cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{
			podTemplate(sharedPodTemplate, append([]core.Container{container(config.ClickHouseContainerName, fipsServerImage)}, containers...)...),
		}}
		if (backup != nil) && (len(containers) > 0) {
			backup.PodTemplate = types.NewString(sharedPodTemplate)
		}
		cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{backupCluster("c", backup)}}
		return cr
	}
	requireViolation := func(t *testing.T, got *chi.ClickHouseInstallation, image string) {
		t.Helper()
		status := got.EnsureStatus()
		require.Equal(t, chi.StatusAborted, status.GetStatus())
		require.Len(t, status.GetErrors(), 1)
		require.True(t, strings.HasPrefix(status.GetErrors()[0], "["+chi.StatusReasonFIPSImagePolicyViolation+"] "), status.GetErrors()[0])
		require.Contains(t, status.GetErrors()[0], image)
		require.Contains(t, status.GetErrors()[0], config.ClickHouseBackupContainerName)
	}

	t.Run("default backup image is no FIPS build", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		got := normalizeBackupCR(t, newCR(enabledBackup()), backupSecretGetter(t))
		requireViolation(t, got, config.DefaultClickHouseBackupDockerImage)
	})
	t.Run("backup without a storage runs no sidecar to hold to the policy", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		got := normalizeBackupCR(t, newCR(&chi.ClusterBackup{Enabled: types.NewStringBool(true)}), backupSecretGetter(t))
		requireNotAborted(t, got)
	})
	t.Run("FIPS image of the sidecar container passes", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		got := normalizeBackupCR(t, newCR(
			enabledBackup(),
			container(config.ClickHouseBackupContainerName, fipsBackupImage),
		), backupSecretGetter(t))
		requireNotAborted(t, got)
	})
	t.Run("sidecar container image is checked", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		got := normalizeBackupCR(t, newCR(
			enabledBackup(),
			container(config.ClickHouseBackupContainerName, userBackupImage),
		), backupSecretGetter(t))
		requireViolation(t, got, userBackupImage)
	})
	t.Run("a sidecar container without an image runs the default one", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		got := normalizeBackupCR(t, newCR(
			enabledBackup(),
			container(config.ClickHouseBackupContainerName, ""),
		), backupSecretGetter(t))
		requireViolation(t, got, config.DefaultClickHouseBackupDockerImage)
	})
	t.Run("an invalid backup pod template is the only error", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		backup := enabledOn("missing")
		requireInvalidPodTemplate(t, normalizeBackupCR(t, newCR(backup), backupSecretGetter(t)), false, `"missing", which does not exist`)
	})
	t.Run("an invalid pod template of the hosts is the only error", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		cr := newCR(nil)
		cr.Spec.Templates.PodTemplates[0] = podTemplate(sharedPodTemplate, container(config.ClickHouseContainerName, ""))
		requireInvalidPodTemplate(t, normalizeBackupCR(t, cr, nil), false, `container "`+config.ClickHouseContainerName+`" without an image`)
	})
	t.Run("an invalid backup pod template hides no violation of the ClickHouse image", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		cr := newCR(enabledOn("missing"))
		cr.Spec.Templates.PodTemplates[0] = podTemplate(sharedPodTemplate, container(config.ClickHouseContainerName, serverImage))
		errs := normalizeBackupCR(t, cr, backupSecretGetter(t)).EnsureStatus().GetErrors()
		require.Len(t, errs, 2, errs)
		var invalid, violation string
		for _, err := range errs {
			switch {
			case strings.HasPrefix(err, "["+chi.StatusReasonInvalidPodTemplate+"] "):
				invalid = err
			case strings.HasPrefix(err, "["+chi.StatusReasonFIPSImagePolicyViolation+"] "):
				violation = err
			}
		}
		require.Contains(t, invalid, `"missing", which does not exist`)
		require.Contains(t, violation, serverImage)
		require.NotContains(t, violation, config.ClickHouseBackupContainerName)
	})
	// newVolumeCR returns an installation of the server image given whose backup pod template has its default-image
	// container use a volume only that pod template declares
	newVolumeCR := func(server string) *chi.ClickHouseInstallation {
		cr := newCR(enabledOn("backup"))
		cr.Spec.Templates.PodTemplates = []chi.PodTemplate{
			podTemplate(sharedPodTemplate, container(config.ClickHouseContainerName, server)),
			withVolume(podTemplate("backup", withVolumeMount(container(config.ClickHouseBackupContainerName, ""), "tls")), "tls"),
		}
		return cr
	}
	t.Run("a volume only the backup pod template declares is the only error", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		requireInvalidPodTemplate(t, normalizeBackupCR(t, newVolumeCR(fipsServerImage), backupSecretGetter(t)), false, `using volume "tls"`)
	})
	t.Run("a volume only the backup pod template declares hides no violation of the ClickHouse image", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		errs := normalizeBackupCR(t, newVolumeCR(serverImage), backupSecretGetter(t)).EnsureStatus().GetErrors()
		require.Len(t, errs, 2, "the template error and the ClickHouse image's violation, not the backup image's: %v", errs)
		var violations []string
		for _, err := range errs {
			if strings.HasPrefix(err, "["+chi.StatusReasonFIPSImagePolicyViolation+"] ") {
				violations = append(violations, err)
			}
		}
		require.Len(t, violations, 1, errs)
		require.Contains(t, violations[0], serverImage)
		require.NotContains(t, violations[0], config.ClickHouseBackupContainerName)
	})
	t.Run("backup disabled - a container of that name is the user's", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		got := normalizeBackupCR(t, newCR(nil, container(config.ClickHouseBackupContainerName, userBackupImage)), nil)
		requireNotAborted(t, got)
	})
	t.Run("Permissive ignores the backup image", func(t *testing.T) {
		withFIPSImagePolicy(t, "")
		got := normalizeBackupCR(t, newCR(enabledBackup()), backupSecretGetter(t))
		requireNotAborted(t, got)
	})
}

// The FIPS image policy reads the image of every backup-enabled host's sidecar as the creator resolves it, so that the
// image it holds to the policy is the one the sidecar and the backup Jobs run. The normalizer resolves it on its own,
// as it must not depend on the creator, so the two rules are compared on every host of a few installations.
func TestBackupSidecarImageAsTheCreatorResolvesIt(t *testing.T) {
	containers := managers.NewContainerManager(managers.ContainerManagerTypeClickHouse)
	server := container(config.ClickHouseContainerName, serverImage)
	for name, newCR := range map[string]func() *chi.ClickHouseInstallation{
		"a sidecar container without an image": func() *chi.ClickHouseInstallation {
			cr := &chi.ClickHouseInstallation{}
			cr.Spec.Backup = enabledOn(sharedPodTemplate)
			cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: sharedPodTemplate}}
			cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{
				podTemplate(sharedPodTemplate, server, container(config.ClickHouseBackupContainerName, "")),
				podTemplate("plain", server),
			}}
			off := backupCluster("c", &chi.ClusterBackup{Enabled: types.NewStringBool(false)})
			off.Templates = &chi.TemplatesList{PodTemplate: "plain"}
			cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{backupCluster("a", nil), off}}
			return cr
		},
		"no pod template": func() *chi.ClickHouseInstallation {
			cr := &chi.ClickHouseInstallation{}
			cr.Spec.Backup = enabledBackup()
			cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{backupCluster("a", nil)}}
			return cr
		},
		"a backup pod template apart from the shards' own ones": func() *chi.ClickHouseInstallation {
			cr := &chi.ClickHouseInstallation{}
			cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{
				podTemplate(sharedPodTemplate, server),
				podTemplate("other", server),
				podTemplate("backup", container(config.ClickHouseBackupContainerName, "registry.example.com/backup:3")),
			}}
			cluster := backupCluster("a", enabledOn("backup"))
			cluster.Templates = &chi.TemplatesList{PodTemplate: sharedPodTemplate}
			cluster.Layout = &chi.ChiClusterLayout{Shards: []*chi.ChiShard{
				{Name: "0", ReplicasCount: 1, Templates: &chi.TemplatesList{PodTemplate: "other"}},
				{Name: "1", ReplicasCount: 1},
			}}
			cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{cluster}}
			return cr
		},
		"the sidecar ahead of ClickHouse": func() *chi.ClickHouseInstallation {
			cr := &chi.ClickHouseInstallation{}
			cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: sharedPodTemplate}}
			cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{
				podTemplate(sharedPodTemplate, container(config.ClickHouseBackupContainerName, ""), container(templateContainer, serverImage)),
			}}
			backup := enabledOn(sharedPodTemplate)
			cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{backupCluster("a", backup)}}
			return cr
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := normalizeBackupCR(t, newCR(), backupSecretGetter(t))
			requireNotAborted(t, got)
			hosts := 0
			got.WalkHosts(func(host *chi.Host) error {
				sidecar, ok := containers.NewBackupContainer(host)
				require.Equal(t, host.GetCluster().GetBackup().IsEnabled(), ok, host.GetName())
				if ok {
					hosts++
					require.Equal(t, sidecar.Image, resolveBackupSidecarImage(host), host.GetName())
				}
				return nil
			})
			require.NotZero(t, hosts, "fixture: a backup-enabled host")
		})
	}
}

// withVolumeMount returns the container mounting the named volume as well
func withVolumeMount(container core.Container, volume string) core.Container {
	container.VolumeMounts = append(slices.Clone(container.VolumeMounts), core.VolumeMount{Name: volume, MountPath: "/mnt/" + volume})
	return container
}

// withVolumeDevice returns the container using the named volume as a raw block device as well
func withVolumeDevice(container core.Container, volume string) core.Container {
	container.VolumeDevices = append(slices.Clone(container.VolumeDevices), core.VolumeDevice{Name: volume, DevicePath: "/dev/" + volume})
	return container
}

// withVolume returns the pod template declaring an emptyDir volume of the name given as well
func withVolume(template chi.PodTemplate, volume string) chi.PodTemplate {
	template.Spec.Volumes = append(slices.Clone(template.Spec.Volumes), core.Volume{
		Name: volume, VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}},
	})
	return template
}

// The sidecar is built on the clickhouse-backup container of the pod template backup.podTemplate names, alike on
// every host of the cluster, whatever pod template each host runs. That is the one container of its name a host
// of a backup-enabled cluster may have; the pod template named has to exist and to hold it once, and the volumes
// it uses have to be the hosts' as well, as the sidecar takes the container alone.
func TestBackupPodTemplate(t *testing.T) {
	const (
		backupTemplate = "backup"
		backupImage    = "registry.example.com/clickhouse-backup:pinned"
	)
	containers := managers.NewContainerManager(managers.ContainerManagerTypeClickHouse)
	server := container(config.ClickHouseContainerName, serverImage)
	sidecar := container(config.ClickHouseBackupContainerName, backupImage, env("LOG_LEVEL", "debug"))
	newCR := func(backup *chi.ClusterBackup, templates ...chi.PodTemplate) *chi.ClickHouseInstallation {
		cr := &chi.ClickHouseInstallation{}
		cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: sharedPodTemplate}}
		cr.Spec.Templates = &chi.Templates{PodTemplates: append([]chi.PodTemplate{podTemplate(sharedPodTemplate, server)}, templates...)}
		cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{backupCluster("c", backup)}}
		return cr
	}
	// requireSidecars requires every host of the installation to run the sidecar of the image and log level given
	requireSidecars := func(t *testing.T, got *chi.ClickHouseInstallation, image, logLevel string) {
		t.Helper()
		requireNotAborted(t, got)
		hosts := 0
		got.WalkHosts(func(host *chi.Host) error {
			built, ok := containers.NewBackupContainer(host)
			require.True(t, ok, host.GetName())
			require.Equal(t, image, built.Image, host.GetName())
			require.Equal(t, image, resolveBackupSidecarImage(host), host.GetName())
			level := ""
			for _, envVar := range built.Env {
				if envVar.Name == "LOG_LEVEL" {
					level = envVar.Value
				}
			}
			require.Equal(t, logLevel, level, host.GetName())
			hosts++
			return nil
		})
		require.NotZero(t, hosts, "fixture: a backup-enabled host")
	}

	t.Run("a pod template of its own, for hosts of other pod templates alike", func(t *testing.T) {
		cr := newCR(enabledOn(backupTemplate), podTemplate(backupTemplate, sidecar), podTemplate("shard", server))
		cr.Spec.Configuration.Clusters[0].Layout = &chi.ChiClusterLayout{Shards: []*chi.ChiShard{
			{Name: "0", ReplicasCount: 1, Templates: &chi.TemplatesList{PodTemplate: "shard"}},
			{Name: "1", ReplicasCount: 1},
		}}
		requireSidecars(t, normalizeBackupCR(t, cr, backupSecretGetter(t)), backupImage, "debug")
	})
	t.Run("the hosts' own pod template", func(t *testing.T) {
		cr := newCR(enabledOn(sharedPodTemplate))
		cr.Spec.Templates.PodTemplates[0] = podTemplate(sharedPodTemplate, server, sidecar)
		requireSidecars(t, normalizeBackupCR(t, cr, backupSecretGetter(t)), backupImage, "debug")
	})
	t.Run("a cluster's empty podTemplate over the installation's - the operator's own sidecar", func(t *testing.T) {
		cr := newCR(&chi.ClusterBackup{PodTemplate: types.NewString("")}, podTemplate(backupTemplate, sidecar))
		cr.Spec.Backup = enabledOn(backupTemplate)
		cr.Spec.Backup.ConfigSecretRef = configSecretRef(backupStorageSecret)
		requireSidecars(t, normalizeBackupCR(t, cr, backupSecretGetter(t)), config.DefaultClickHouseBackupDockerImage, "")
	})
	t.Run("a cluster's empty podTemplate over the installation's, without a storage - no sidecar", func(t *testing.T) {
		cr := newCR(&chi.ClusterBackup{PodTemplate: types.NewString("")}, podTemplate(backupTemplate, sidecar))
		cr.Spec.Backup = enabledOn(backupTemplate)
		cr.Spec.Backup.ConfigSecretRef = nil
		got := normalizeBackupCR(t, cr, backupSecretGetter(t))
		requireNotAborted(t, got)
		require.True(t, findCluster(t, got, "c").GetBackup().LacksStorage())
		got.WalkHosts(func(host *chi.Host) error {
			_, ok := containers.NewBackupContainer(host)
			require.False(t, ok, host.GetName())
			return nil
		})
		requireNoBackupUser(t, got)
	})
	t.Run("from a ClickHouseInstallationTemplate", func(t *testing.T) {
		tpl := newCHIT("chit-backup-pod", "")
		tpl.Spec.Backup = enabledOn(backupTemplate)
		tpl.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{podTemplate(backupTemplate, sidecar)}}
		registerCHIT(t, tpl)
		cr := newCR(nil)
		cr.Spec.UseTemplates = []*chi.TemplateRef{{Name: tpl.Name}}
		requireSidecars(t, normalizeBackupCR(t, cr, backupSecretGetter(t)), backupImage, "debug")
	})

	for name, tc := range map[string]struct {
		backup    *chi.ClusterBackup
		templates []chi.PodTemplate
		hostOwn   bool
		details   []string
	}{
		"a clickhouse-backup container of the hosts' pod template, none named": {
			backup:  enabledBackup(),
			hostOwn: true,
			details: []string{`pod template "` + sharedPodTemplate + `" has container "` + config.ClickHouseBackupContainerName + `" on cluster "c", whose spec enables backup`, backupPodTemplateHint},
		},
		// The container is meant for the sidecar, which naming its pod template turns on: it is not run as written
		"a clickhouse-backup container of the hosts' pod template, backup enabled without storage": {
			backup:  &chi.ClusterBackup{Enabled: types.NewStringBool(true)},
			hostOwn: true,
			details: []string{`pod template "` + sharedPodTemplate + `" has container "` + config.ClickHouseBackupContainerName + `" on cluster "c", whose spec enables backup`, backupPodTemplateHint},
		},
		"a clickhouse-backup container of the hosts' pod template, another one named": {
			backup:    enabledOn(backupTemplate),
			templates: []chi.PodTemplate{podTemplate(backupTemplate, sidecar)},
			hostOwn:   true,
			details:   []string{`pod template "` + sharedPodTemplate + `" has container "` + config.ClickHouseBackupContainerName + `"`, backupPodTemplateHint},
		},
		"the pod template named does not exist": {
			backup:  enabledOn("missing"),
			details: []string{backupPodTemplateField + ` of cluster "c" names pod template "missing", which does not exist`},
		},
		"the pod template named has no clickhouse-backup container": {
			backup:    enabledOn(backupTemplate),
			templates: []chi.PodTemplate{podTemplate(backupTemplate, server)},
			details:   []string{`names pod template "` + backupTemplate + `", which has no container "` + config.ClickHouseBackupContainerName + `"`},
		},
		"the pod template named has two clickhouse-backup containers": {
			backup:    enabledOn(backupTemplate),
			templates: []chi.PodTemplate{podTemplate(backupTemplate, sidecar, sidecar)},
			details:   []string{`names pod template "` + backupTemplate + `", which has two containers named "` + config.ClickHouseBackupContainerName + `"`},
		},
		"a volume only the pod template named declares": {
			backup:    enabledOn(backupTemplate),
			templates: []chi.PodTemplate{withVolume(podTemplate(backupTemplate, withVolumeMount(sidecar, "tls")), "tls")},
			details: []string{`names pod template "` + backupTemplate + `", which has container "` + config.ClickHouseBackupContainerName +
				`" using volume "tls", which only that pod template declares, not pod template "` + sharedPodTemplate + `" the hosts run`, backupPodTemplateVolumeHint},
		},
		"a volume device only the pod template named declares": {
			backup:    enabledOn(backupTemplate),
			templates: []chi.PodTemplate{withVolume(podTemplate(backupTemplate, withVolumeDevice(sidecar, "raw")), "raw")},
			details:   []string{`using volume "raw", which only that pod template declares`},
		},
	} {
		t.Run(name, func(t *testing.T) {
			cr := newCR(tc.backup, tc.templates...)
			if tc.hostOwn {
				cr.Spec.Templates.PodTemplates[0] = podTemplate(sharedPodTemplate, server, container(config.ClickHouseBackupContainerName, ""))
			}
			requireInvalidPodTemplate(t, normalizeBackupCR(t, cr, backupSecretGetter(t)), false, tc.details...)
		})
	}
	t.Run("a volume the pods have as well", func(t *testing.T) {
		cr := newCR(enabledOn(backupTemplate), withVolume(podTemplate(backupTemplate, withVolumeMount(sidecar, "tls")), "tls"))
		cr.Spec.Templates.PodTemplates[0] = withVolume(cr.Spec.Templates.PodTemplates[0], "tls")
		requireSidecars(t, normalizeBackupCR(t, cr, backupSecretGetter(t)), backupImage, "debug")
	})
	t.Run("a volume only the pod template named declares, on a shard of another pod template", func(t *testing.T) {
		backupWithVolume := withVolume(podTemplate(backupTemplate, server, withVolumeMount(sidecar, "tls")), "tls")
		cr := newCR(enabledOn(backupTemplate), backupWithVolume, podTemplate("other", server))
		cr.Spec.Configuration.Clusters[0].Layout = &chi.ChiClusterLayout{Shards: []*chi.ChiShard{
			{Name: "0", ReplicasCount: 1, Templates: &chi.TemplatesList{PodTemplate: backupTemplate}},
			{Name: "1", ReplicasCount: 1, Templates: &chi.TemplatesList{PodTemplate: "other"}},
		}}
		requireInvalidPodTemplate(t, normalizeBackupCR(t, cr, backupSecretGetter(t)), false, `not pod template "other" the hosts run`)
	})
	t.Run("a volume only the pod template named declares, the hosts running none", func(t *testing.T) {
		cr := newCR(enabledOn(backupTemplate), withVolume(podTemplate(backupTemplate, withVolumeMount(sidecar, "tls")), "tls"))
		cr.Spec.Defaults = nil
		requireInvalidPodTemplate(t, normalizeBackupCR(t, cr, backupSecretGetter(t)), false, `which only that pod template declares, while the hosts run no pod template`)
	})
	t.Run("a volume no pod template declares is left to the operator and the API server", func(t *testing.T) {
		cr := newCR(enabledOn(backupTemplate), podTemplate(backupTemplate, withVolumeMount(sidecar, config.ClickHouseBackupSharedDataVolumeName)))
		requireSidecars(t, normalizeBackupCR(t, cr, backupSecretGetter(t)), backupImage, "debug")
	})
	t.Run("a volume claim template of the installation", func(t *testing.T) {
		cr := newCR(enabledOn(backupTemplate), withVolume(podTemplate(backupTemplate, withVolumeMount(sidecar, "disk")), "disk"))
		cr.Spec.Templates.VolumeClaimTemplates = []chi.VolumeClaimTemplate{{Name: "disk"}}
		requireSidecars(t, normalizeBackupCR(t, cr, backupSecretGetter(t)), backupImage, "debug")
	})
	t.Run("backup off - a pod template named is no concern", func(t *testing.T) {
		requireNotAborted(t, normalizeBackupCR(t, newCR(&chi.ClusterBackup{PodTemplate: types.NewString("missing")}), nil))
	})
}

// The normalizer prepares the CR the creator builds objects from, so it must not depend on the creator:
// whatever it needs to know about backup it asks the API types.
func TestNormalizerDoesNotImportCreator(t *testing.T) {
	const creatorPackage = "github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	sources, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, source, nil, parser.ImportsOnly)
		require.NoError(t, err)
		for _, spec := range file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			require.NoError(t, err)
			require.NotEqual(t, creatorPackage, path, "%s imports the creator", source)
		}
	}
}

// IsBackupEnabled replays the inheritance from .spec.backup, so it answers alike for an installation as
// written - which is what the normalizer sees when it decides on the backup user - and once normalized.
func TestIsBackupEnabled(t *testing.T) {
	for name, tc := range map[string]struct {
		spec    *chi.ClusterBackup
		cluster *chi.ClusterBackup
		want    bool
	}{
		"nothing set":                {want: false},
		"enabled for the CR":         {spec: enabledBackup(), want: true},
		"enabled for the cluster":    {cluster: enabledBackup(), want: true},
		"cluster opts out of the CR": {spec: enabledBackup(), cluster: &chi.ClusterBackup{Enabled: types.NewStringBool(false)}, want: false},
		"enabled without storage":    {spec: &chi.ClusterBackup{Enabled: types.NewStringBool(true)}, want: false},
		"storage of the CR, enabled for the cluster": {
			spec:    &chi.ClusterBackup{ConfigSecretRef: configSecretRef(backupStorageSecret)},
			cluster: &chi.ClusterBackup{Enabled: types.NewStringBool(true)},
			want:    true,
		},
		"cluster drops the storage of the CR": {
			spec:    enabledBackup(),
			cluster: &chi.ClusterBackup{ConfigSecretRef: configSecretRef("")},
			want:    false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cr := &chi.ClickHouseInstallation{}
			cr.Spec.Backup = tc.spec
			cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{backupCluster("c", tc.cluster)}}
			require.Equal(t, tc.want, cr.IsBackupEnabled(), "as written")
			require.Equal(t, tc.want, normalizeBackupCR(t, cr.DeepCopy(), backupSecretGetter(t)).IsBackupEnabled(), "normalized")
		})
	}
}

// Without operator-managed backup a user named like the backup user is the installation's own - e.g. one a
// hand-made sidecar logs in as - and gets the same default networks as any other user.
func TestBackupUsernameWithoutBackupIsUsers(t *testing.T) {
	const ordinaryUser = "reader"
	cr := &chi.ClickHouseInstallation{}
	cr.Spec.Configuration = &chi.Configuration{
		Users: users(
			backupUser(userFieldPassword), "chosen-by-user",
			ordinaryUser+"/"+userFieldPassword, "chosen-by-user",
		),
		Clusters: []*chi.Cluster{backupCluster("c", nil)},
	}

	got := normalizeBackupCR(t, cr, nil)
	users := got.Spec.Configuration.Users
	require.True(t, users.Has(ordinaryUser+"/"+userFieldNetworksIP), "fixture: an ordinary user gets default networks")
	for _, field := range []string{userFieldNetworksIP, userFieldNetworksHostRegexp} {
		require.Equal(t, users.Get(ordinaryUser+"/"+field), users.Get(backupUser(field)), field)
	}
}

// The operator serves the sidecar's REST API over plain HTTP to the backup Jobs: an API_SECURE the pod template's
// clickhouse-backup container turns on literally is refused rather than turned off unasked; one from elsewhere is
// pinned off. A cluster without operator-managed backup runs a container of that name as its author wrote it, TLS
// included. Whichever container a template lists first, the sidecar is never taken for ClickHouse, so no order is
// refused, and a template holding nothing but the sidecar gets the default ClickHouse container.
func TestBackupContainerValidation(t *testing.T) {
	newCR := func(backup bool, containers ...core.Container) *chi.ClickHouseInstallation {
		cr := &chi.ClickHouseInstallation{}
		cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: sharedPodTemplate}}
		cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{podTemplate(sharedPodTemplate, containers...)}}
		clusterBackup := &chi.ClusterBackup{Enabled: types.NewStringBool(backup)}
		if backup {
			clusterBackup.PodTemplate = types.NewString(sharedPodTemplate)
			clusterBackup.ConfigSecretRef = configSecretRef(backupStorageSecret)
		}
		cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{backupCluster("c", clusterBackup)}}
		return cr
	}
	server := container(config.ClickHouseContainerName, "clickhouse/clickhouse-server")
	apiSecure := func(values ...string) core.Container {
		var envVars []core.EnvVar
		for _, value := range values {
			envVars = append(envVars, env(config.ClickHouseBackupEnvAPISecure, value))
		}
		return container(config.ClickHouseBackupContainerName, "", envVars...)
	}
	// kubelet sets the last of two entries of one name
	for _, values := range [][]string{{"true"}, {"1"}, {"TRUE"}, {"false", "true"}} {
		t.Run(fmt.Sprintf("refused %q", values), func(t *testing.T) {
			got := normalizeBackupCR(t, newCR(true, server, apiSecure(values...)), backupSecretGetter(t))
			requireInvalidPodTemplate(t, got, false, config.ClickHouseBackupEnvAPISecure+"="+values[len(values)-1])
		})
	}
	for _, values := range [][]string{{"false"}, {"0"}, {""}, {"true", "false"}} {
		t.Run(fmt.Sprintf("accepted %q", values), func(t *testing.T) {
			requireNotAborted(t, normalizeBackupCR(t, newCR(true, server, apiSecure(values...)), backupSecretGetter(t)))
		})
	}
	selfManaged := container(config.ClickHouseBackupContainerName, "altinity/clickhouse-backup", env(config.ClickHouseBackupEnvAPISecure, "true"))
	requireNotAborted(t, normalizeBackupCR(t, newCR(false, server, selfManaged), nil))

	clickHousePod := container(templateContainer, serverImage)
	requireNotAborted(t, normalizeBackupCR(t, newCR(true, container(config.ClickHouseBackupContainerName, ""), clickHousePod), backupSecretGetter(t)))
	requireNotAborted(t, normalizeBackupCR(t, newCR(true, container(config.ClickHouseBackupContainerName, "")), backupSecretGetter(t)))
	requireNotAborted(t, normalizeBackupCR(t, newCR(true, clickHousePod, container(config.ClickHouseBackupContainerName, "")), backupSecretGetter(t)))
}

// The backup Secret names the backup user: the operator creates the user of that name, from the pod only, and a user
// of the default name is then the installation's own, with the default networks of any user
func TestBackupUserNamedBySecret(t *testing.T) {
	const named = "backup.operator-1"
	cr := &chi.ClickHouseInstallation{}
	cr.Spec.Configuration = &chi.Configuration{
		Users: users(
			named+"/"+userFieldNetworksIP, "::/0",
			named+"/"+userFieldProfile, "backup-profile",
			backupUser(userFieldPassword), "chosen-by-user",
			"reader/"+userFieldPassword, "chosen-by-user",
		),
		Clusters: []*chi.Cluster{backupCluster("c", enabledBackup())},
	}

	got := normalizeBackupCR(t, cr, func(namespace, name string) (*core.Secret, error) {
		return &core.Secret{Data: map[string][]byte{
			config.ClickHouseBackupSecretKeyClickHouseUsername: []byte(named),
			config.ClickHouseBackupSecretKeyClickHousePassword: []byte(backupSecretPassword),
		}}, nil
	})
	requireNotAborted(t, got)
	users := got.Spec.Configuration.Users
	hash := sha256.Sum256([]byte(backupSecretPassword))
	require.Equal(t, hex.EncodeToString(hash[:]), users.Get(named+"/"+userFieldPasswordSHA256Hex).String())
	require.ElementsMatch(t, config.ClickHouseBackupUserNetworksIP, users.Get(named+"/"+userFieldNetworksIP).AsVectorOfStrings())
	require.False(t, users.Has(named+"/"+userFieldNetworksHostRegexp))
	require.Equal(t, "backup-profile", users.Get(named+"/"+userFieldProfile).String())
	require.NotEqual(t, hex.EncodeToString(hash[:]), users.Get(backupUser(userFieldPasswordSHA256Hex)).String())
	require.True(t, users.Has("reader/"+userFieldNetworksIP), "fixture: an ordinary user gets default networks")
	for _, field := range []string{userFieldNetworksIP, userFieldNetworksHostRegexp} {
		require.Equal(t, users.Get("reader/"+field), users.Get(backupUser(field)), "a user of the default name is the installation's own: %s", field)
	}
}

// A backup Secret naming a user the backup user cannot be aborts the CR, rather than take over the default user or
// the operator's own, or break the users configuration
func TestBackupUserNamedInvalidly(t *testing.T) {
	for name, username := range map[string]string{
		"default user":  "default",
		"operator user": chop.Config().ClickHouse.Access.Username,
		"invalid name":  "backup user",
		"leading digit": "1backup",
		"path":          "backup/password",
	} {
		t.Run(name, func(t *testing.T) {
			cr := &chi.ClickHouseInstallation{}
			cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{backupCluster("c", enabledBackup())}}
			got := normalizeBackupCR(t, cr, func(namespace, name string) (*core.Secret, error) {
				return &core.Secret{Data: map[string][]byte{
					config.ClickHouseBackupSecretKeyClickHouseUsername: []byte(username),
					config.ClickHouseBackupSecretKeyClickHousePassword: []byte(backupSecretPassword),
				}}, nil
			})
			status := got.EnsureStatus()
			require.Equal(t, chi.StatusAborted, status.GetStatus())
			require.True(t, strings.HasPrefix(status.GetErrors()[0], "["+chi.StatusReasonBackupSecretUnresolved+"] "), status.GetErrors()[0])
			require.Contains(t, status.GetErrors()[0], config.ClickHouseBackupSecretKeyClickHouseUsername)
		})
	}
}

// testBackupUsername is the ClickHouse username the backup Secrets of the tests set: the operator has none of its own
const testBackupUsername = "backup_user"
