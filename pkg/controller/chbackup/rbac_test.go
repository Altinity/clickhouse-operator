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
	batch "k8s.io/api/batch/v1"
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
		// The cache watches the Jobs; a Job is created per operation, patched by settle and
		// observeScheduledJob, and read past the cache by settle.
		{batch.GroupName, "jobs", []string{"get", "list", "watch", "create", "patch"}},
		// applyCronJob creates and updates the CronJobs of a schedule, suspendCronJob patches them, and
		// dropLeavingClusters deletes the CronJob of a cluster the schedule no longer backs up.
		{batch.GroupName, "cronjobs", []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
		// jobPods reads the pods of a Job past the cache.
		{core.GroupName, "pods", []string{"list"}},
		{core.GroupName, "events", []string{"create"}},
		// An installation is read together with the normalized state of its last completed reconcile,
		// which its status ConfigMap holds.
		{core.GroupName, "configmaps", []string{"get"}},
		// The recovery controller marks the installation it recovered.
		{clickhouse_altinity_com.APIGroupName, "clickhouseinstallations", []string{"get", "list", "watch", "patch"}},
		{clickhouse_altinity_com.APIGroupName, "clickhousebackups", []string{"get", "list", "watch"}},
		{clickhouse_altinity_com.APIGroupName, "clickhousebackupschedules", []string{"get", "list", "watch"}},
		// The recovery controller creates the restore a recovery runs.
		{clickhouse_altinity_com.APIGroupName, "clickhouserestores", []string{"get", "list", "watch", "create"}},
		{clickhouse_altinity_com.APIGroupName, "clickhousebackups/status", []string{"update"}},
		{clickhouse_altinity_com.APIGroupName, "clickhousebackupschedules/status", []string{"update"}},
		{clickhouse_altinity_com.APIGroupName, "clickhouserestores/status", []string{"update"}},
		// Jobs, CronJobs and the restore a recovery runs block the deletion of the resource that controls
		// them, which needs an update of its finalizers where the OwnerReferencesPermissionEnforcement
		// admission plugin is on.
		{clickhouse_altinity_com.APIGroupName, "clickhouseinstallations/finalizers", []string{"update"}},
		{clickhouse_altinity_com.APIGroupName, "clickhousebackups/finalizers", []string{"update"}},
		{clickhouse_altinity_com.APIGroupName, "clickhousebackupschedules/finalizers", []string{"update"}},
		{clickhouse_altinity_com.APIGroupName, "clickhouserestores/finalizers", []string{"update"}},
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
