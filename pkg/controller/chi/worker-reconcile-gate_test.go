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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func alwaysFalse() bool { return false }
func alwaysTrue() bool  { return true }

func TestDecideReconcileGate(t *testing.T) {
	tests := []struct {
		name string
		in   reconcileGateInputs
		want reconcileGateDecision
	}{
		{
			// THE REGRESSION. A client PUTs the whole object, setting a spec field (e.g. stop)
			// and stripping the operator's finalizer in one request. updateCHI re-adds the
			// finalizer and returns without reconciling, so the spec change is only ever
			// delivered on the following event - which is generation-same, because a finalizer
			// write is metadata-only. Skipping it strands the request silently: no status write,
			// no event, no failure metric, and callers polling status.taskIDsCompleted wait
			// forever.
			name: "spec change arrives with a finalizer re-add, generation unchanged",
			in: reconcileGateInputs{
				hasReconcileWork:            true,
				afterFinalizerInstalled:     true,
				generationTheSame:           true,
				operatorIPTheSame:           true,
				hasHostNeedingStuckRecovery: alwaysFalse,
			},
			want: gateFinalizerInstalled,
		},
		{
			// The steady state as production actually presents it. hasReconcileWork is true on
			// EVERY event at gate time (hosts are still ObjectStatusUnknown until
			// PrepareHostStatefulSetWithStatus runs, so HasDrift is always true), so it must not
			// outrank the skip - if it does, the skip never fires, and the operator's own status
			// writes re-enqueue the CR into a self-sustaining reconcile loop.
			name: "converged CR, generation unchanged, hasReconcileWork set as production always sets it",
			in: reconcileGateInputs{
				hasReconcileWork:            true,
				generationTheSame:           true,
				operatorIPTheSame:           true,
				hasHostNeedingStuckRecovery: alwaysFalse,
			},
			want: gateNothingToDo,
		},
		{
			// Same shape, but the action plan is empty - the finalizer re-add alone must still
			// carry the reconcile, which is what reconcile-1/reconcile-2 have always been for.
			name: "finalizer re-add alone, generation unchanged",
			in: reconcileGateInputs{
				afterFinalizerInstalled:     true,
				generationTheSame:           true,
				operatorIPTheSame:           true,
				hasHostNeedingStuckRecovery: alwaysFalse,
			},
			want: gateFinalizerInstalled,
		},
		{
			name: "finalizer re-add observed against the completed ancestor",
			in: reconcileGateInputs{
				afterFinalizerInstalledAncestor: true,
				generationTheSame:               true,
				operatorIPTheSame:               true,
				hasHostNeedingStuckRecovery:     alwaysFalse,
			},
			want: gateFinalizerInstalled,
		},
		{
			// The steady state: converged CR, operator's own status writes re-enqueue it.
			name: "converged CR with nothing pending",
			in: reconcileGateInputs{
				generationTheSame:           true,
				operatorIPTheSame:           true,
				hasHostNeedingStuckRecovery: alwaysFalse,
			},
			want: gateNothingToDo,
		},
		{
			// a1be5bb63 - the clickhouse-operator user's networks/host_regexp carry the
			// operator's pod IP and must be refreshed when it moves.
			name: "operator IP changed",
			in: reconcileGateInputs{
				generationTheSame:           true,
				operatorIPTheSame:           false,
				hasHostNeedingStuckRecovery: alwaysFalse,
			},
			want: gateOperatorIPChanged,
		},
		{
			// #1704 - an unhealthy host must not be skipped just because the spec is unchanged.
			name: "unhealthy host with unchanged spec",
			in: reconcileGateInputs{
				generationTheSame:           true,
				hasUnhealthyHosts:           true,
				operatorIPTheSame:           true,
				hasHostNeedingStuckRecovery: alwaysFalse,
			},
			want: gateUnhealthyHosts,
		},
		{
			name: "sustained-NotReady host with unchanged spec",
			in: reconcileGateInputs{
				generationTheSame:           true,
				hasUnhealthyHosts:           true,
				operatorIPTheSame:           true,
				hasHostNeedingStuckRecovery: alwaysTrue,
			},
			want: gateStuckHostRecovery,
		},
		{
			name: "ordinary spec change",
			in: reconcileGateInputs{
				hasReconcileWork:            true,
				operatorIPTheSame:           true,
				hasHostNeedingStuckRecovery: alwaysFalse,
			},
			want: gateReconcileWork,
		},
		{
			// Defensive: reconcileCR always supplies the callback, but the decision must not
			// panic if a future caller omits it.
			name: "nil stuck-recovery callback is tolerated",
			in: reconcileGateInputs{
				generationTheSame: true,
				operatorIPTheSame: true,
			},
			want: gateNothingToDo,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, decideReconcileGate(tt.in))
		})
	}
}

// The skip decisions are the only ones that must not proceed - everything else continues into
// the reconcile. Stated separately so a future decision added to the enum has to be classified.
func TestReconcileGateDecisionProceeds(t *testing.T) {
	proceeds := map[reconcileGateDecision]bool{
		gateNothingToDo:        false,
		gateReconcileWork:      true,
		gateFinalizerInstalled: true,
		gateOperatorIPChanged:  true,
		gateStuckHostRecovery:  true,
		gateUnhealthyHosts:     true,
	}
	for decision, want := range proceeds {
		require.Equal(t, want, decision.proceeds(), "decision %s", decision)
	}
}
