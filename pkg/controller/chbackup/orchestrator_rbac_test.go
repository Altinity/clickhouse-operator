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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// withOperatorNamespace runs the test as an operator in the namespace.
func withOperatorNamespace(t *testing.T, namespace string) {
	t.Helper()
	former := operatorNamespace
	operatorNamespace = func() string { return namespace }
	forgetOrchestratorRBAC()
	t.Cleanup(func() {
		operatorNamespace = former
		forgetOrchestratorRBAC()
	})
}

func orchestratorObjects(t *testing.T, c client.Client, namespace string) (*core.ServiceAccount, *rbac.Role, *rbac.RoleBinding) {
	t.Helper()
	ctx := context.Background()
	get := func(name string, obj client.Object) client.Object {
		err := c.Get(ctx, kubeTypes.NamespacedName{Namespace: namespace, Name: name}, obj)
		if apiErrors.IsNotFound(err) {
			return nil
		}
		require.NoError(t, err)
		return obj
	}
	sa, _ := get(OrchestratorServiceAccountName(), &core.ServiceAccount{}).(*core.ServiceAccount)
	role, _ := get(OrchestratorRoleName, &rbac.Role{}).(*rbac.Role)
	binding, _ := get(OrchestratorRoleName, &rbac.RoleBinding{}).(*rbac.RoleBinding)
	return sa, role, binding
}

// The orchestrator runs as the operator's ServiceAccount: in the namespace of the operator that is the operator's
// own, which nothing is done to; elsewhere the operator makes one of the same name, bound to a Role that grants the
// orchestrator what it does, marked as made by the operator
func TestEnsureOrchestratorServiceAccount(t *testing.T) {
	ctx := context.Background()
	t.Run("namespace of the operator", func(t *testing.T) {
		withOperatorNamespace(t, testNamespace)
		c := newClient(t, nil)
		name, err := EnsureOrchestratorServiceAccount(ctx, c, c, testNamespace)
		require.NoError(t, err)
		require.Equal(t, defaultOperatorServiceAccount, name)
		sa, role, binding := orchestratorObjects(t, c, testNamespace)
		require.Nil(t, sa)
		require.Nil(t, role)
		require.Nil(t, binding)
	})
	t.Run("another namespace", func(t *testing.T) {
		withOperatorNamespace(t, "operator")
		c := newClient(t, nil)
		name, err := EnsureOrchestratorServiceAccount(ctx, c, c, testNamespace)
		require.NoError(t, err)
		require.Equal(t, OrchestratorServiceAccountName(), name)
		sa, role, binding := orchestratorObjects(t, c, testNamespace)
		require.NotNil(t, sa)
		require.True(t, isOperatorManaged(sa))
		require.NotNil(t, role)
		require.True(t, isOperatorManaged(role))
		require.Equal(t, orchestratorRules, role.Rules)
		require.NotNil(t, binding)
		require.Equal(t, rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: OrchestratorRoleName}, binding.RoleRef)
		require.Equal(t, []rbac.Subject{{Kind: rbac.ServiceAccountKind, Name: name, Namespace: testNamespace}}, binding.Subjects)
	})
	t.Run("drifted objects are put right", func(t *testing.T) {
		withOperatorNamespace(t, "operator")
		labels := orchestratorLabels()
		c := newClient(t, nil,
			&rbac.Role{ObjectMeta: meta.ObjectMeta{Namespace: testNamespace, Name: OrchestratorRoleName, Labels: labels},
				Rules: []rbac.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get"}}}},
			&rbac.RoleBinding{ObjectMeta: meta.ObjectMeta{Namespace: testNamespace, Name: OrchestratorRoleName, Labels: labels},
				RoleRef: rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "ClusterRole", Name: "view"}})
		_, err := EnsureOrchestratorServiceAccount(ctx, c, c, testNamespace)
		require.NoError(t, err)
		_, role, binding := orchestratorObjects(t, c, testNamespace)
		require.Equal(t, orchestratorRules, role.Rules)
		require.Equal(t, "Role", binding.RoleRef.Kind)
		require.Len(t, binding.Subjects, 1)
	})
	t.Run("objects of another owner are not taken", func(t *testing.T) {
		withOperatorNamespace(t, "operator")
		c := newClient(t, nil,
			&core.ServiceAccount{ObjectMeta: meta.ObjectMeta{Namespace: testNamespace, Name: OrchestratorServiceAccountName()}},
			&rbac.Role{ObjectMeta: meta.ObjectMeta{Namespace: testNamespace, Name: OrchestratorRoleName}})
		_, err := EnsureOrchestratorServiceAccount(ctx, c, c, testNamespace)
		require.ErrorContains(t, err, "not made by the operator")
		sa, role, _ := orchestratorObjects(t, c, testNamespace)
		require.False(t, isOperatorManaged(sa), "a ServiceAccount of the name is used as it is")
		require.Empty(t, role.Rules)
	})
}

// The controller of the backup Pods ensures the ServiceAccount of the orchestrator in the namespace of every
// backup-enabled installation, and nowhere else
func TestShardPodControllerEnsuresOrchestratorServiceAccount(t *testing.T) {
	withOperatorNamespace(t, "operator")
	chi := podModeInstallation(t, 1)
	r, c := newShardPodController(t, chi)
	_, err := r.Reconcile(context.Background(), request(testCHI))
	require.NoError(t, err)
	sa, role, binding := orchestratorObjects(t, c, testNamespace)
	require.NotNil(t, sa)
	require.NotNil(t, role)
	require.NotNil(t, binding)

	forgetOrchestratorRBAC()
	off := installation(t, api.StatusCompleted, backupCluster("main", 1, false))
	r, c = newShardPodController(t, off)
	_, err = r.Reconcile(context.Background(), request(testCHI))
	require.NoError(t, err)
	sa, _, _ = orchestratorObjects(t, c, testNamespace)
	require.Nil(t, sa)
}
