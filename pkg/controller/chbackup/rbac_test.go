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
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	coordination "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"

	clickhouse_altinity_com "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
)

// operatorRoleTemplatePath is the source of the operator's Role and ClusterRole. The install bundles,
// the Helm chart and the OperatorHub bundle are generated from it.
const operatorRoleTemplatePath = "../../../deploy/builder/templates-install-bundle/clickhouse-operator-install-yaml-template-02-section-rbac-02-role.yaml"

// rbacRule is a rule of the operator's role as the template writes it.
type rbacRule struct {
	APIGroups     []string `yaml:"apiGroups"`
	Resources     []string `yaml:"resources"`
	ResourceNames []string `yaml:"resourceNames"`
	Verbs         []string `yaml:"verbs"`
}

// allows reports whether the rule grants the verb on every object of the resource.
func (r rbacRule) allows(group, resource, verb string) bool {
	return (len(r.ResourceNames) == 0) && slices.Contains(r.APIGroups, group) && slices.Contains(r.Resources, resource) &&
		(slices.Contains(r.Verbs, verb) || slices.Contains(r.Verbs, rbac.VerbAll))
}

// loadOperatorRoleRules returns the rules of the operator's role.
func loadOperatorRoleRules(t *testing.T) []rbacRule {
	t.Helper()
	raw, err := os.ReadFile(operatorRoleTemplatePath)
	require.NoError(t, err)
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	for {
		var doc struct {
			Rules []rbacRule `yaml:"rules"`
		}
		if err := dec.Decode(&doc); err != nil {
			break
		}
		if len(doc.Rules) > 0 {
			return doc.Rules
		}
	}
	t.Fatalf("no rules found in %s", operatorRoleTemplatePath)
	return nil
}

// The operator's role grants every call the backup controllers make: a fake client enforces no RBAC, so a
// missing verb shows only on a real cluster, where the call is refused and the resource is reconciled again
// and again without progress.
func TestOperatorRoleGrantsBackupControllers(t *testing.T) {
	required := []struct {
		group    string
		resource string
		verbs    []string
	}{
		{core.GroupName, "events", []string{"create"}},
		// An installation is read together with the normalized state of its last completed reconcile, which its
		// status ConfigMap holds; the storage path check reads the keys of the Secrets and ConfigMaps the sidecars
		// take their settings from.
		{core.GroupName, "configmaps", []string{"get"}},
		{core.GroupName, "secrets", []string{"get"}},
		// The backup controllers are leader-elected
		{coordination.GroupName, "leases", []string{"get", "create", "update"}},
		// The recovery controller marks the installation it recovered.
		{clickhouse_altinity_com.APIGroupName, "clickhouseinstallations", []string{"get", "list", "watch", "patch"}},
		// The controllers set and remove their finalizers by a patch.
		{clickhouse_altinity_com.APIGroupName, "clickhousebackups", []string{"get", "list", "watch", "update", "patch"}},
		{clickhouse_altinity_com.APIGroupName, "clickhousebackupschedules", []string{"get", "list", "watch", "update", "patch"}},
		// The recovery controller creates the restore a recovery runs.
		{clickhouse_altinity_com.APIGroupName, "clickhouserestores", []string{"get", "list", "watch", "create", "update", "patch"}},
		{clickhouse_altinity_com.APIGroupName, "clickhousebackups/status", []string{"update"}},
		{clickhouse_altinity_com.APIGroupName, "clickhousebackupschedules/status", []string{"update"}},
		{clickhouse_altinity_com.APIGroupName, "clickhouserestores/status", []string{"update"}},
		// The restore a recovery runs blocks the deletion of the installation that controls it, which needs an update
		// of its finalizers where the OwnerReferencesPermissionEnforcement admission plugin is on.
		{clickhouse_altinity_com.APIGroupName, "clickhouseinstallations/finalizers", []string{"update"}},
	}

	rules := loadOperatorRoleRules(t)
	for _, r := range required {
		for _, verb := range r.verbs {
			granted := slices.ContainsFunc(rules, func(rule rbacRule) bool {
				return rule.allows(r.group, r.resource, verb)
			})
			require.True(t, granted, "the operator role does not grant %q on %q in API group %q", verb, r.resource, r.group)
		}
	}
}

// The Lease the backup controllers are elected by is in the namespace of the operator: the cluster-wide role does not
// grant it, a Role of that namespace does. The template marks the rule as the one a ClusterRole drops, and the
// install bundles and the Helm chart grant it through a Role: see the builder and the chart.
func TestLeasesAreGrantedByANamespacedRoleOnly(t *testing.T) {
	raw, err := os.ReadFile(operatorRoleTemplatePath)
	require.NoError(t, err)
	template := string(raw)
	begin, end := strings.Index(template, "# begin namespaced-only"), strings.Index(template, "# end namespaced-only")
	require.True(t, (begin >= 0) && (end > begin), "the lease rule is marked as the one a ClusterRole drops")
	require.Equal(t, 1, strings.Count(template, "- leases"))
	require.Contains(t, template[begin:end], "- leases")

	for _, path := range []string{
		"../../../deploy/builder/templates-install-bundle/clickhouse-operator-install-yaml-template-02-section-rbac-03-leader-election.yaml",
	} {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Contains(t, string(raw), "kind: Role\n")
		require.Contains(t, string(raw), "kind: RoleBinding\n")
		require.Contains(t, string(raw), "- leases")
	}
}

// The operator drives clickhouse-backup through SQL: it runs no Job, no CronJob and no Pod of its own, creates no
// ServiceAccount, Role or RoleBinding, and its role grants none of that.
func TestOperatorRoleGrantsNoBackupWorkloads(t *testing.T) {
	rules := loadOperatorRoleRules(t)
	for _, denied := range []struct {
		group, resource, verb string
	}{
		{"batch", "jobs", "create"},
		{"batch", "cronjobs", "create"},
		{core.GroupName, "pods", "create"},
		{core.GroupName, "serviceaccounts", "create"},
		{rbac.GroupName, "roles", "create"},
		{rbac.GroupName, "rolebindings", "create"},
		{core.GroupName, "pods/exec", "create"},
	} {
		require.False(t, slices.ContainsFunc(rules, func(rule rbacRule) bool { return rule.allows(denied.group, denied.resource, denied.verb) }),
			"the operator role grants %q on %q in API group %q", denied.verb, denied.resource, denied.group)
	}
}
