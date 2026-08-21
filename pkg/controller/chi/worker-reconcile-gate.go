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

package chi

// reconcileGateDecision is why reconcileCR proceeds with - or skips - a reconcile.
type reconcileGateDecision string

const (
	// gateReconcileWork: the action plan carries work not yet applied.
	gateReconcileWork reconcileGateDecision = "ReconcileWork"
	// gateFinalizerInstalled: the operator just re-added its finalizer. Installing a finalizer is
	// a metadata-only write, so it does not bump metadata.generation - the event therefore looks
	// generation-same even though the spec change that arrived with it has never been reconciled.
	gateFinalizerInstalled reconcileGateDecision = "FinalizerInstalled"
	// gateOperatorIPChanged: the operator's pod IP moved, so the clickhouse-operator user's
	// networks/host_regexp must be refreshed.
	gateOperatorIPChanged reconcileGateDecision = "OperatorIPChanged"
	// gateStuckHostRecovery: a host has been sustained-NotReady past its threshold.
	gateStuckHostRecovery reconcileGateDecision = "StuckHostRecovery"
	// gateUnhealthyHosts: a host is unhealthy, so the shard may need recovery (#1704).
	gateUnhealthyHosts reconcileGateDecision = "UnhealthyHosts"
	// gateNothingToDo: nothing above applied, skip.
	gateNothingToDo reconcileGateDecision = "NothingToDo"
)

// proceeds reports whether the decision continues into a reconcile.
func (d reconcileGateDecision) proceeds() bool {
	switch d {
	case gateNothingToDo:
		return false
	}
	return true
}

// reconcileGateInputs are the observations decideReconcileGate branches on. Grouped into a struct
// so the decision is a pure function that can be exhaustively tested - it is easy to get the
// ORDER of these checks wrong, and the cost of doing so is silent: a dropped reconcile writes no
// status, emits no event, and increments no failure metric.
type reconcileGateInputs struct {
	// hasReconcileWork is the action plan comparing the CR against its last completed ancestor.
	hasReconcileWork bool
	// afterFinalizerInstalled is computed from the raw informer pair BEFORE buildCR replaces the
	// CR with its normalized form - the finalizer transition is only visible there.
	afterFinalizerInstalled bool
	// afterFinalizerInstalledAncestor is the same check against the last completed ancestor.
	afterFinalizerInstalledAncestor bool
	generationTheSame               bool
	hasUnhealthyHosts               bool
	operatorIPTheSame               bool
	// hasHostNeedingStuckRecovery is lazy: it costs live pod reads, so it is only consulted once
	// the cheaper reasons have been ruled out.
	hasHostNeedingStuckRecovery func() bool
}

// decideReconcileGate decides whether reconcileCR proceeds.
//
// ORDER MATTERS, and the ordering is the point of this function. Every reason to DO work is
// checked before the generation-same skip, because a same-generation event can still carry
// unapplied work:
//
//   - Kubernetes bumps metadata.generation only on spec writes. A finalizer re-add is a metadata
//     write, so the event that follows it is generation-same.
//   - A client that PUTs a full object can strip the operator's finalizer along with a spec
//     change. updateCHI re-adds the finalizer and returns without reconciling, so the spec change
//     is only ever delivered on that following generation-same event.
//
// Skipping it there strands the spec change with no status write, no event and no failure metric.
// That is how an external `stop` request was silently dropped and left callers polling
// status.taskIDsCompleted forever.
//
// hasReconcileWork sits BELOW the generation-same skip on purpose, and moving it above would
// disable the skip entirely. At the point reconcileCR consults this gate no host has been through
// PrepareHostStatefulSetWithStatus, so every host still carries ObjectStatusUnknown; HasDrift is
// therefore true and HasReconcileWork returns true on every single event, converged or not. It
// discriminates nothing here - its only job is to carry generation-CHANGED events past the skip,
// which is why it must not be deleted either.
func decideReconcileGate(in reconcileGateInputs) reconcileGateDecision {
	switch {
	case in.afterFinalizerInstalled, in.afterFinalizerInstalledAncestor:
		return gateFinalizerInstalled
	case !in.operatorIPTheSame:
		return gateOperatorIPChanged
	case in.generationTheSame && !in.hasUnhealthyHosts:
		return gateNothingToDo
	case in.hasReconcileWork:
		return gateReconcileWork
	case (in.hasHostNeedingStuckRecovery != nil) && in.hasHostNeedingStuckRecovery():
		return gateStuckHostRecovery
	case in.hasUnhealthyHosts:
		return gateUnhealthyHosts
	default:
		return gateNothingToDo
	}
}
