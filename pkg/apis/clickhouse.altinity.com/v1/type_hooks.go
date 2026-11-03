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

package v1

import (
	"strings"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// HookTarget identifies where to execute a cluster-level hook action. Alias of
// types.String so existing pointer/Value()/MergeFrom semantics work unchanged;
// the named alias only narrows the field's documented domain to the constants below.
//
// Valid values: HookTargetFirstHost (default), HookTargetAllHosts, HookTargetAllShards.
// Case-insensitive at runtime: "FirstHost" and "firsthost" both normalize to
// HookTargetFirstHost. Ignored for host-level hooks — they always run on the host
// being reconciled.
//
// Codegen note: gengo v2 cannot emit deepcopy for fields typed as a Go alias, so
// HookAction.Target is declared as *types.String (the underlying type) rather than
// *HookTarget. Both types are interchangeable at the language level. The associated
// "unsupported type" warning gengo prints during package parse is filtered out by
// dev/run_code_generator.sh.
type HookTarget = types.String

// HookFailurePolicy controls how a hook action's error is propagated. Alias of
// types.String so existing pointer/Value()/MergeFrom semantics work unchanged.
//
// Valid values: HookFailurePolicyFail (default), HookFailurePolicyIgnore.
// Case-insensitive at runtime: "Fail" and "fail" both normalize to
// HookFailurePolicyFail; "Ignore" and "ignore" both to HookFailurePolicyIgnore.
type HookFailurePolicy = types.String

const (
	// HookTargetFirstHost runs the action on cluster.FirstHost() only. Default for cluster hooks.
	HookTargetFirstHost HookTarget = "FirstHost"
	// HookTargetAllHosts runs the action on every host in the cluster.
	HookTargetAllHosts HookTarget = "AllHosts"
	// HookTargetAllShards runs the action on the first replica of each shard.
	HookTargetAllShards HookTarget = "AllShards"

	// HookFailurePolicyFail (default) propagates any hook execution error to the caller.
	// For pre-hooks this aborts the reconcile / deletion; for post-hooks the error is logged.
	HookFailurePolicyFail HookFailurePolicy = "Fail"
	// HookFailurePolicyIgnore swallows hook execution errors with a warning log. Useful for
	// best-effort drains where a stuck hook should not block deletion of an already-broken host.
	HookFailurePolicyIgnore HookFailurePolicy = "Ignore"
)

// NewHookTarget builds a HookTarget value from a plain string. Use over a raw HookTarget(s)
// cast at call sites to make typed-domain construction obvious to readers.
func NewHookTarget(s string) HookTarget {
	return HookTarget(s)
}

// NewHookFailurePolicy builds a HookFailurePolicy value from a plain string. Sibling of
// NewHookTarget — see that function for rationale.
func NewHookFailurePolicy(s string) HookFailurePolicy {
	return HookFailurePolicy(s)
}

// GetTarget returns the resolved HookTarget for this action: the explicit value if set,
// otherwise HookTargetFirstHost (the documented default for cluster-level hooks).
// The returned value is NORMALIZED — case variants like "FirstHost" and "firsthost"
// both map to HookTargetFirstHost, so callers can switch on the typed constants
// directly with == comparison. Nil-safe.
func (a *HookAction) GetTarget() HookTarget {
	//Default value
	if (a == nil) || (a.Target == nil) || !a.Target.HasValue() {
		return HookTargetFirstHost
	}
	// Normalized actual value
	return normalizeHookTarget(NewHookTarget(a.Target.Value()))
}

// normalizeHookTarget maps an arbitrarily-cased HookTarget to the canonical PascalCase
// form (HookTargetFirstHost / HookTargetAllHosts / HookTargetAllShards). Unknown values
// pass through unchanged so the runtime can surface a clear "unknown target" error
// rather than silently coercing to the default.
func normalizeHookTarget(t HookTarget) HookTarget {
	for _, normalized := range []HookTarget{
		HookTargetFirstHost,
		HookTargetAllHosts,
		HookTargetAllShards,
	} {
		if t.EqualFold(&normalized) {
			return normalized
		}
	}
	return t
}

// GetFailurePolicy returns the resolved HookFailurePolicy for this action: the explicit
// value if set, otherwise HookFailurePolicyFail (the documented default).
// The returned value is NORMALIZED — case variants like "Fail" and "fail" both
// map to HookFailurePolicyFail, so callers can compare against the typed constants
// directly with == comparison. Nil-safe.
func (a *HookAction) GetFailurePolicy() HookFailurePolicy {
	// Default value
	if (a == nil) || (a.FailurePolicy == nil) || !a.FailurePolicy.HasValue() {
		return HookFailurePolicyFail
	}
	// Normalized actual value
	return normalizeHookFailurePolicy(NewHookFailurePolicy(a.FailurePolicy.Value()))
}

// normalizeHookFailurePolicy maps an arbitrarily-cased HookFailurePolicy to the canonical
// PascalCase form (HookFailurePolicyFail / HookFailurePolicyIgnore). Unknown values pass
// through unchanged so the runtime can surface a clear "unknown failurePolicy" error
// rather than silently coercing to the default.
func normalizeHookFailurePolicy(p HookFailurePolicy) HookFailurePolicy {
	for _, normalized := range []HookFailurePolicy{
		HookFailurePolicyFail,
		HookFailurePolicyIgnore,
	} {
		if p.EqualFold(&normalized) {
			return normalized
		}
	}
	return p
}

// HookEvent identifies a specific reconcile lifecycle event that a hook may opt into.
// A hook with `events: [eventA, eventB]` fires only when the classifier emits at least one
// of the listed events.
//
// Events are grouped into two SCOPES that match the YAML hook nesting:
//
//	Host-scope events (Host* prefix) apply to host-level hooks defined at
//	  spec.reconcile.host.hooks (CHI level, inherited by clusters) — the operator
//	  evaluates them per host during host reconcile.
//
//	Cluster-scope events (Cluster* prefix) apply to cluster-level hooks defined at
//	  spec.configuration.clusters[N].reconcile.hooks — the operator evaluates them
//	  once per cluster reconcile, around the host iteration.
//
// The two scopes are completely independent. A cluster-level hook listening to a Host*
// event is rejected at CRD validation time, and vice versa. The shared `Any` event
// is the only one valid in both scopes.
//
// Case-insensitive: "Any" and "any", "HostCreate" and "hostcreate" are equivalent
// at runtime. The CRD enum lists both PascalCase and all-lowercase variants so
// either form is accepted at apply time. Constants below use canonical PascalCase.
type HookEvent string

// String returns the underlying string value. Implements fmt.Stringer so HookEvent
// is printable via %s and avoids ad-hoc string() casts at use sites.
func (e HookEvent) String() string {
	return string(e)
}

// EqualFold reports whether two HookEvent values are equal under
// case-insensitive comparison (uses strings.EqualFold).
func (e HookEvent) EqualFold(other HookEvent) bool {
	return strings.EqualFold(e.String(), other.String())
}

const (
	// HookEventAny is the wildcard event — matches every event the classifier emits.
	// Valid in BOTH host and cluster scopes. In practice: a hook listening to [Any]
	// fires on every host reconcile (host scope) or every cluster reconcile (cluster
	// scope), and on the pre-delete sweep on a dying host (host scope).
	//
	// Naming rationale: "Any" describes the matching semantic (any fired event
	// satisfies the hook). Avoid "Always" — Kubernetes uses that word with a
	// different temporal meaning (Pod.spec.restartPolicy, imagePullPolicy).
	HookEventAny HookEvent = "Any"

	// --- Host-scope events (spec.reconcile.host.hooks) ---

	// HookEventHostCreate fires on the very first reconcile that creates a host
	// (host has no ancestor). Best paired with POST hooks: the host's pod is up and SQL
	// can run. PRE hooks listing only [HostCreate] are silently inert today — pre-hooks
	// are skipped on first creation because there is no live pod to talk to yet.
	HookEventHostCreate HookEvent = "HostCreate"
	// HookEventHostDelete fires on the reconcile that removes a host from the cluster
	// (host is present in the ancestor spec but absent from the current spec). Emitted
	// only from the dedicated delete sweep (worker-deleter.go runHostPreDeleteHooks).
	// Always emitted alongside HookEventHostShutdown.
	HookEventHostDelete HookEvent = "HostDelete"
	// HookEventHostUpdate fires on a reconcile that has prior state for the host
	// (i.e. neither create nor delete). Catch-all for "the host was already there and
	// is being reconciled again".
	HookEventHostUpdate HookEvent = "HostUpdate"

	// HookEventHostStart fires when a host transitions from stopped to running
	// (ancestor was stopped, current is not).
	HookEventHostStart HookEvent = "HostStart"
	// HookEventHostStop fires when a host is being stopped (current spec marks it
	// stopped, regardless of prior state).
	HookEventHostStop HookEvent = "HostStop"

	// HookEventHostConfigRestart fires when the operator decides this host needs an
	// in-place software restart for a configuration change to take effect.
	HookEventHostConfigRestart HookEvent = "HostConfigRestart"
	// HookEventHostRollout fires when a pod-template change forces a StatefulSet
	// rollout (e.g. new env var, new image, new volume).
	HookEventHostRollout HookEvent = "HostRollout"

	// HookEventHostShutdown is an aggregate convenience: fires whenever the pod is
	// going to be brought down for any reason — Stop, Delete, ConfigRestart, or Rollout.
	// Use for "before this host's pod goes away, do X" patterns (e.g. swarm leave,
	// graceful drain).
	HookEventHostShutdown HookEvent = "HostShutdown"

	// --- Cluster-scope events (spec.configuration.clusters[N].reconcile.hooks) ---

	// HookEventClusterCreate fires on a cluster reconcile where ALL hosts are new
	// (first time the operator sees the cluster — no host has an ancestor). Best
	// paired with POST hooks: hosts are up and SQL works.
	HookEventClusterCreate HookEvent = "ClusterCreate"
	// HookEventClusterDelete is reserved for the planned cluster-delete sweep wiring
	// and is NOT currently emitted by the runtime. A hook listening to [ClusterDelete]
	// today silently never fires. Will fire on cluster removal once the dedicated
	// sweep (parallel to firedHostDeleteEvents) is wired up.
	HookEventClusterDelete HookEvent = "ClusterDelete"
	// HookEventClusterReconcile fires on every cluster reconcile pass that the
	// operator actually runs against an existing cluster (i.e. at least one host has
	// prior state). It does NOT mean "cluster spec was modified" — taskID-only
	// reconciles, scale-ups (where one host has prior state), and ordinary applies
	// all emit ClusterReconcile. The operator's upstream gates (isGenerationTheSame,
	// HasReconcileWork) already filter out no-op informer resyncs, so this event
	// only fires when the operator decided there is work.
	//
	// Use ClusterReconcile for "every reconcile pass over the cluster" patterns
	// (logging, periodic SYSTEM FLUSH LOGS). For "only when something specific
	// changed" semantics, listen to host-scope events at host-level instead.
	HookEventClusterReconcile HookEvent = "ClusterReconcile"
)

// HostScopeEvents is the set of events valid in `events:` lists on host-level hooks
// (spec.reconcile.host.hooks). The matching CRD enum mirrors this list.
var HostScopeEvents = []HookEvent{
	HookEventAny,
	HookEventHostCreate,
	HookEventHostDelete,
	HookEventHostUpdate,
	HookEventHostStart,
	HookEventHostStop,
	HookEventHostConfigRestart,
	HookEventHostRollout,
	HookEventHostShutdown,
}

// ClusterScopeEvents is the set of events valid in `events:` lists on cluster-level hooks
// (spec.configuration.clusters[N].reconcile.hooks). The matching CRD enum mirrors this.
var ClusterScopeEvents = []HookEvent{
	HookEventAny,
	HookEventClusterCreate,
	HookEventClusterDelete,
	HookEventClusterReconcile,
}

// HookAction defines one action to execute at a reconcile lifecycle point.
// Exactly one of the action type fields must be specified.
type HookAction struct {
	// SQL executes SQL queries against ClickHouse.
	// +optional
	SQL *SQLHookAction `json:"sql,omitempty" yaml:"sql,omitempty"`
	// Shell executes a command inside the pod. Not yet implemented.
	// +optional
	Shell *ShellHookAction `json:"shell,omitempty" yaml:"shell,omitempty"`
	// HTTP makes an HTTP request to an endpoint. Not yet implemented.
	// +optional
	HTTP *HTTPHookAction `json:"http,omitempty" yaml:"http,omitempty"`
	// Target specifies which host(s) to execute this action on, for cluster-level hooks.
	// See HookTarget for valid values and defaults.
	// Stored as *types.String for codegen reasons (see HookTarget).
	// Use a.GetTarget() for the typed read.
	// +optional
	Target *types.String `json:"target,omitempty" yaml:"target,omitempty"`
	// Events lists the reconcile events that should trigger this action. Required, must
	// be non-empty. Use "Any" to fire on every reconcile (wildcard match). See HookEvent
	// constants for the full list. A hook with no matching events on a given reconcile
	// is silently skipped — no SQL/HTTP/shell call is made.
	// +kubebuilder:validation:MinItems=1
	Events []HookEvent `json:"events,omitempty" yaml:"events,omitempty"`
	// FailurePolicy controls what happens when this action returns an error.
	// See HookFailurePolicy for valid values and defaults. Behavior matrix:
	//   Pre-hook with Fail:    error aborts the reconcile / host deletion.
	//   Pre-hook with Ignore:  error is logged as a warning, reconcile continues.
	//   Post-hook with Fail:   error short-circuits the post-hook iteration; the outer
	//                          reconcile path logs the error but does not abort, since
	//                          post-hooks run after the reconcile work is already done.
	//   Post-hook with Ignore: error is logged as a warning, the next post-hook still runs.
	// Useful for best-effort drains on a possibly-broken host (Ignore on a delete pre-hook).
	// Stored as *types.String for codegen reasons (see HookTarget).
	// Use a.GetFailurePolicy() for the typed read.
	// +optional
	FailurePolicy *types.String `json:"failurePolicy,omitempty" yaml:"failurePolicy,omitempty"`
}

// ShouldIgnoreFailure reports whether errors from this action should be swallowed with a
// warning instead of propagated. Default (no value) is to propagate.
func (a *HookAction) ShouldIgnoreFailure() bool {
	return a.GetFailurePolicy() == HookFailurePolicyIgnore
}

// MatchesAnyEvent reports whether this action should fire given the set of events
// that fired during the current reconcile (produced by firedHostEvents /
// firedClusterEvents). Returns false if the action's Events list is empty (which
// is invalid input, but the runtime treats it as "never fire" — schema validation
// is the user-facing enforcement point).
func (a *HookAction) MatchesAnyEvent(list []HookEvent) bool {
	if a == nil {
		// HookAction is nil - nothing to do.
		return false
	}
	if len(a.Events) == 0 {
		// HookAction has no events to fire upon - nothing to do.
		return false
	}

	// Iterate over the action's desired events.
	// Comparison is case-insensitive (HookEvent.EqualFold) so the user can write
	// "Any"/"any" or "HostCreate"/"hostcreate" interchangeably.
	for _, hookActionTriggeredByEvent := range a.Events {
		// Special case: Any is the wildcard — matches without consulting the emitted events.
		if hookActionTriggeredByEvent.EqualFold(HookEventAny) {
			return true
		}

		// Check if the emitted events contain the desired event.
		for _, curEvent := range list {
			if hookActionTriggeredByEvent.EqualFold(curEvent) {
				return true
			}
		}
	}
	// No match found
	return false
}

// IsEmpty returns true if the action has no recognized type set.
func (a *HookAction) IsEmpty() bool {
	// No action type specified - HookAction is empty
	return !a.HasSQL() && !a.HasShell() && !a.HasHTTP()
}

// HasSQL returns true if the action is a SQL hook.
func (a *HookAction) HasSQL() bool {
	if a == nil {
		return false
	}
	return a.SQL != nil
}

// HasShell returns true if the action is a Shell hook.
func (a *HookAction) HasShell() bool {
	if a == nil {
		return false
	}
	return a.Shell != nil
}

// HasHTTP returns true if the action is an HTTP hook.
func (a *HookAction) HasHTTP() bool {
	if a == nil {
		return false
	}
	return a.HTTP != nil
}

// SQLHookAction executes SQL queries against ClickHouse.
type SQLHookAction struct {
	// Queries is a list of SQL statements to execute sequentially.
	Queries []string `json:"queries,omitempty" yaml:"queries,omitempty"`
}

// ShellHookAction executes a command inside a pod container.
// Reserved for future implementation.
type ShellHookAction struct {
	// Command is the command and its arguments.
	Command []string `json:"command,omitempty" yaml:"command,omitempty"`
	// Container specifies the container to run the command in. Defaults to the ClickHouse container.
	// +optional
	Container *types.String `json:"container,omitempty" yaml:"container,omitempty"`
}

// HTTPHookAction makes an HTTP request to an endpoint.
// Reserved for future implementation.
type HTTPHookAction struct {
	// URL is the target endpoint.
	URL *types.String `json:"url,omitempty" yaml:"url,omitempty"`
	// Method is the HTTP method. Defaults to GET.
	// +optional
	Method *types.String `json:"method,omitempty" yaml:"method,omitempty"`
}

// ReconcileHooks defines pre/post actions for a reconcile lifecycle scope.
type ReconcileHooks struct {
	// Pre is a list of actions to execute before the reconcile step.
	// +optional
	Pre []*HookAction `json:"pre,omitempty" yaml:"pre,omitempty"`
	// Post is a list of actions to execute after the reconcile step.
	// +optional
	Post []*HookAction `json:"post,omitempty" yaml:"post,omitempty"`
}

// GetPre returns pre-hooks or nil.
func (h *ReconcileHooks) GetPre() []*HookAction {
	if h == nil {
		return nil
	}
	return h.Pre
}

// GetPost returns post-hooks or nil.
func (h *ReconcileHooks) GetPost() []*HookAction {
	if h == nil {
		return nil
	}
	return h.Post
}

// IsEmpty returns true if there are no pre or post hooks.
func (h *ReconcileHooks) IsEmpty() bool {
	return !h.HasPre() && !h.HasPost()
}

// HasPre returns true if there are any pre-hooks.
func (h *ReconcileHooks) HasPre() bool {
	return len(h.GetPre()) > 0
}

// HasPost returns true if there are any post-hooks.
func (h *ReconcileHooks) HasPost() bool {
	return len(h.GetPost()) > 0
}

// MergeFrom merges hooks from a parent scope.
// Actions from parent are appended AFTER the receiver's own actions, so iteration
// order at execution time is: child's actions first, then parent's. Parent actions
// already present in the child (per HookAction.Equal) are skipped.
func (h *ReconcileHooks) MergeFrom(from *ReconcileHooks) *ReconcileHooks {
	// No parent hooks to merge from - return the receiver as is.
	if from == nil {
		return h
	}

	// No receiver hooks to merge into - return a deep copy of the parent.
	if h == nil {
		return from.DeepCopy()
	}

	h.Pre = mergeHookActions(h.Pre, from.Pre)
	h.Post = mergeHookActions(h.Post, from.Post)

	return h
}

// mergeHookActions appends actions from parent that are not already present in child.
// Idempotent on repeated calls: a parent action whose deep-equal copy is already in
// the child slice is skipped. This is critical because the operator's normalization
// pipeline may invoke inheritance multiple times per reconcile (e.g. buildCR calls
// createTemplatedCR which calls cluster.InheritClusterReconcileFrom, sometimes
// twice with templates/IPs), and the result is persisted into NormalizedCRCompleted
// — without dedup, hooks would accumulate across reconciles and fire N times.
func mergeHookActions(to, from []*HookAction) []*HookAction {
	for _, src := range from {
		if src == nil {
			continue
		}
		// If the action is already in the list, skip it.
		if isHookActionsContains(to, src) {
			continue
		}
		// Action is not in the list - add it.
		to = append(to, src.DeepCopy())
	}

	return to
}

// isHookActionsContains reports whether the slice already holds an action equal to
// the candidate. Equality uses field-by-field comparison via HookAction.Equal.
func isHookActionsContains(list []*HookAction, candidate *HookAction) bool {
	for _, item := range list {
		if item.Equal(candidate) {
			return true
		}
	}

	return false
}

// Equal reports whether a and other describe the same HookAction. Field-by-field
// check that respects the nil-pointer semantics of the action subtypes
// (SQL/Shell/HTTP/Target/FailurePolicy/Events). Used by mergeHookActions to dedup
// inherited entries; relies on the value-identical clones DeepCopy produces during
// the inheritance pipeline.
func (a *HookAction) Equal(other *HookAction) bool {
	if (a == nil) || (other == nil) {
		return a == other
	}
	if !a.SQL.Equal(other.SQL) {
		return false
	}
	if !a.Shell.Equal(other.Shell) {
		return false
	}
	if !a.HTTP.Equal(other.HTTP) {
		return false
	}
	// Target and FailurePolicy are normalized at runtime (case-insensitive), so dedup
	// must use EqualFold here too — otherwise "FirstHost" and "firsthost" or
	// "Fail" and "fail" would be treated as distinct hooks during inheritance merge.
	if !a.Target.EqualFold(other.Target) {
		return false
	}
	if !a.FailurePolicy.EqualFold(other.FailurePolicy) {
		return false
	}
	// Events list is order-insensitive AND case-insensitive — runtime matching uses
	// HookEvent.EqualFold, so ["Any"] and ["any"] describe the same hook for dedup.
	if !util.SlicesEqualAsSetFunc(a.Events, other.Events, HookEvent.EqualFold) {
		return false
	}
	return true
}

// Equal reports whether a and other describe the same SQLHookAction.
func (a *SQLHookAction) Equal(other *SQLHookAction) bool {
	if (a == nil) || (other == nil) {
		return a == other
	}

	if len(a.Queries) != len(other.Queries) {
		return false
	}

	// Check queries are equal.
	// Order does matter.
	for i := range a.Queries {
		if a.Queries[i] != other.Queries[i] {
			return false
		}
	}

	return true
}

// Equal reports whether a and other describe the same ShellHookAction.
func (a *ShellHookAction) Equal(other *ShellHookAction) bool {
	if (a == nil) || (other == nil) {
		return a == other
	}

	if len(a.Command) != len(other.Command) {
		return false
	}

	// Check commands are equal.
	// Order does matter.
	for i := range a.Command {
		if a.Command[i] != other.Command[i] {
			return false
		}
	}

	return a.Container.Equal(other.Container)
}

// Equal reports whether a and other describe the same HTTPHookAction.
func (a *HTTPHookAction) Equal(other *HTTPHookAction) bool {
	if (a == nil) || (other == nil) {
		return a == other
	}

	return a.URL.Equal(other.URL) && a.Method.Equal(other.Method)
}

// MergeFrom merges a HookAction from a parent, filling empty fields.
func (a *HookAction) MergeFrom(from *HookAction) *HookAction {
	if from == nil {
		return a
	}

	if a == nil {
		return from.DeepCopy()

	}
	if a.SQL == nil {
		a.SQL = from.SQL
	} else if from.SQL != nil {
		a.SQL.MergeFrom(from.SQL)
	}

	if a.Shell == nil {
		a.Shell = from.Shell
	} else if from.Shell != nil {
		a.Shell.MergeFrom(from.Shell)
	}

	if a.HTTP == nil {
		a.HTTP = from.HTTP
	} else if from.HTTP != nil {
		a.HTTP.MergeFrom(from.HTTP)
	}

	a.Target = a.Target.MergeFrom(from.Target)
	// Events list is required at the schema level;
	// fill from parent only if receiver is empty.
	// Treat as fill-empty-only to preserve child's explicit choice when both are set.
	if (len(a.Events) == 0) && (len(from.Events) > 0) {
		a.Events = append([]HookEvent(nil), from.Events...)
	}

	a.FailurePolicy = a.FailurePolicy.MergeFrom(from.FailurePolicy)

	return a
}

// MergeFrom merges SQL hook from parent, appending queries.
func (s *SQLHookAction) MergeFrom(from *SQLHookAction) {
	if (from == nil) || (s == nil) {
		return
	}

	s.Queries = append(s.Queries, from.Queries...)
}

// MergeFrom merges Shell hook from parent, appending commands and filling empty container.
func (sh *ShellHookAction) MergeFrom(from *ShellHookAction) {
	if (from == nil) || (sh == nil) {
		return
	}

	sh.Command = append(sh.Command, from.Command...)
	sh.Container = sh.Container.MergeFrom(from.Container)
}

// MergeFrom merges HTTP hook from parent, filling empty fields.
func (h *HTTPHookAction) MergeFrom(from *HTTPHookAction) {
	if (from == nil) || (h == nil) {
		return
	}

	h.URL = h.URL.MergeFrom(from.URL)
	h.Method = h.Method.MergeFrom(from.Method)
}
