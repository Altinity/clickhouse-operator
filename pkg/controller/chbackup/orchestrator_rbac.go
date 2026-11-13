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
	"fmt"
	"os"
	"sync"
	"time"

	core "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	clickhouse_altinity_com "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

const (
	// OrchestratorRoleName names the Role, and the RoleBinding, that let the backup orchestrator do what it does in
	// the namespace of an installation other than the operator's own.
	OrchestratorRoleName = "clickhouse-operator-backup-orchestrator"
	// defaultOperatorServiceAccount is the ServiceAccount of the operator as the install bundles name it, taken when
	// the operator runs without OPERATOR_POD_SERVICE_ACCOUNT, outside a pod say.
	defaultOperatorServiceAccount = "clickhouse-operator"
	// backupComponentOrchestratorRBAC is the model.LabelBackupComponent of the ServiceAccount, Role and RoleBinding
	// of the orchestrator, which marks them as made by the operator.
	backupComponentOrchestratorRBAC = "orchestrator-rbac"
	// labelManagedBy is the well-known label of the tool that manages an object.
	labelManagedBy = "app.kubernetes.io/managed-by"
	// managedByOperator is the labelManagedBy of what the operator makes.
	managedByOperator = "clickhouse-operator"
	// orchestratorRBACRecheckInterval is how long the operator trusts a namespace it ensured the orchestrator's
	// ServiceAccount in before it reads it again: every reconcile of every installation asks for it.
	orchestratorRBACRecheckInterval = 10 * time.Minute
)

// orchestratorRules are what the backup orchestrator does through the Kubernetes API: it posts Events on the backup
// resource it runs for, reads the resources of the backup, and reads pods. The operator holds every one of these
// rules itself, as Kubernetes lets it grant no more than it holds.
var orchestratorRules = []rbac.PolicyRule{
	{APIGroups: []string{core.GroupName}, Resources: []string{"events"}, Verbs: []string{"create"}},
	{APIGroups: []string{core.GroupName}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}},
	{
		APIGroups: []string{clickhouse_altinity_com.APIGroupName},
		Resources: []string{"clickhousebackupschedules", "clickhousebackups", "clickhouserestores"},
		Verbs:     []string{"get"},
	},
}

// orchestratorRBACEnsured holds, by namespace, when the orchestrator's ServiceAccount was last ensured there.
var orchestratorRBACEnsured sync.Map

// OrchestratorServiceAccountName returns the name of the ServiceAccount the backup orchestrator Jobs run as: the
// operator's own, OPERATOR_POD_SERVICE_ACCOUNT, in every namespace.
func OrchestratorServiceAccountName() string {
	if name := operatorRuntimeParam(deployment.OPERATOR_POD_SERVICE_ACCOUNT); name != "" {
		return name
	}
	return defaultOperatorServiceAccount
}

// operatorNamespace returns the namespace the operator runs in, "" when unknown. A variable for the tests.
var operatorNamespace = func() string {
	return operatorRuntimeParam(deployment.OPERATOR_POD_NAMESPACE)
}

// operatorRuntimeParam returns a runtime parameter of the operator, read from the environment when the operator
// has not been set up, in tests say.
func operatorRuntimeParam(name string) string {
	if chop.Get() != nil {
		if value, ok := chop.GetRuntimeParam(name); ok {
			return value
		}
		return ""
	}
	return os.Getenv(name)
}

// orchestratorLabels are the labels of the ServiceAccount, Role and RoleBinding of the orchestrator.
func orchestratorLabels() map[string]string {
	return map[string]string{
		model.LabelBackupComponent: backupComponentOrchestratorRBAC,
		labelManagedBy:             managedByOperator,
	}
}

// isOperatorManaged reports whether the operator made the object, as its labels tell.
func isOperatorManaged(obj meta.Object) bool {
	return obj.GetLabels()[model.LabelBackupComponent] == backupComponentOrchestratorRBAC
}

// EnsureOrchestratorServiceAccount makes sure the backup orchestrator Jobs can run in the namespace as the
// ServiceAccount whose name it returns, see OrchestratorServiceAccountName. In the namespace of the operator that
// is the operator's own ServiceAccount, which nothing is done to. In any other namespace the operator keeps a
// ServiceAccount of that name and the Role and RoleBinding OrchestratorRoleName that grant it orchestratorRules.
// A ServiceAccount of that name the operator did not make is used as it is; a Role or RoleBinding of that name it
// did not make is an error, never taken over.
func (r *reconciler) EnsureOrchestratorServiceAccount(ctx context.Context, namespace string) (string, error) {
	return EnsureOrchestratorServiceAccount(ctx, r.Client, r.apiReader, namespace)
}

// EnsureOrchestratorServiceAccount is reconciler.EnsureOrchestratorServiceAccount with the clients given: c writes,
// reader reads past any cache.
func EnsureOrchestratorServiceAccount(ctx context.Context, c client.Client, reader client.Reader, namespace string) (string, error) {
	name := OrchestratorServiceAccountName()
	if namespace == operatorNamespace() {
		return name, nil
	}
	if at, ok := orchestratorRBACEnsured.Load(namespace); ok && (time.Since(at.(time.Time)) < orchestratorRBACRecheckInterval) {
		return name, nil
	}
	if err := ensureOrchestratorServiceAccount(ctx, c, reader, namespace, name); err != nil {
		return name, err
	}
	if err := ensureOrchestratorRole(ctx, c, reader, namespace); err != nil {
		return name, err
	}
	if err := ensureOrchestratorRoleBinding(ctx, c, reader, namespace, name); err != nil {
		return name, err
	}
	orchestratorRBACEnsured.Store(namespace, time.Now())
	return name, nil
}

// forgetOrchestratorRBAC makes the next EnsureOrchestratorServiceAccount read every namespace anew, for the tests.
func forgetOrchestratorRBAC() {
	orchestratorRBACEnsured.Range(func(key, _ any) bool {
		orchestratorRBACEnsured.Delete(key)
		return true
	})
}

func ensureOrchestratorServiceAccount(ctx context.Context, c client.Client, reader client.Reader, namespace, name string) error {
	current := &core.ServiceAccount{}
	err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, current)
	switch {
	case err == nil:
		return nil
	case !apiErrors.IsNotFound(err):
		return err
	}
	sa := &core.ServiceAccount{ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: name, Labels: orchestratorLabels()}}
	if err := c.Create(ctx, sa); err != nil && !apiErrors.IsAlreadyExists(err) {
		return err
	}
	log.V(1).Info("created ServiceAccount %s/%s of the backup orchestrator", namespace, name)
	return nil
}

func ensureOrchestratorRole(ctx context.Context, c client.Client, reader client.Reader, namespace string) error {
	desired := &rbac.Role{
		ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: OrchestratorRoleName, Labels: orchestratorLabels()},
		Rules:      orchestratorRules,
	}
	current := &rbac.Role{}
	err := reader.Get(ctx, client.ObjectKeyFromObject(desired), current)
	switch {
	case apiErrors.IsNotFound(err):
		if err := c.Create(ctx, desired); err != nil && !apiErrors.IsAlreadyExists(err) {
			return err
		}
		return nil
	case err != nil:
		return err
	case !isOperatorManaged(current):
		return fmt.Errorf("Role %s/%s exists and was not made by the operator", namespace, OrchestratorRoleName)
	case equality.Semantic.DeepEqual(current.Rules, desired.Rules):
		return nil
	}
	current.Rules = desired.Rules
	return c.Update(ctx, current)
}

func ensureOrchestratorRoleBinding(ctx context.Context, c client.Client, reader client.Reader, namespace, serviceAccount string) error {
	desired := &rbac.RoleBinding{
		ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: OrchestratorRoleName, Labels: orchestratorLabels()},
		RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: OrchestratorRoleName},
		Subjects:   []rbac.Subject{{Kind: rbac.ServiceAccountKind, Name: serviceAccount, Namespace: namespace}},
	}
	current := &rbac.RoleBinding{}
	err := reader.Get(ctx, client.ObjectKeyFromObject(desired), current)
	switch {
	case apiErrors.IsNotFound(err):
		if err := c.Create(ctx, desired); err != nil && !apiErrors.IsAlreadyExists(err) {
			return err
		}
		return nil
	case err != nil:
		return err
	case !isOperatorManaged(current):
		return fmt.Errorf("RoleBinding %s/%s exists and was not made by the operator", namespace, OrchestratorRoleName)
	case current.RoleRef != desired.RoleRef:
		// The role a binding refers to cannot be changed: the binding is made anew
		if err := c.Delete(ctx, current, client.Preconditions{UID: &current.UID}); client.IgnoreNotFound(err) != nil {
			return err
		}
		return c.Create(ctx, desired)
	case equality.Semantic.DeepEqual(current.Subjects, desired.Subjects):
		return nil
	}
	current.Subjects = desired.Subjects
	return c.Update(ctx, current)
}
