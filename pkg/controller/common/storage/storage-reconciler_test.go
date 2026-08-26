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

package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// A volume that was merely ADDED must never outrank a volume that was LOST. ReconcilePVCs walks a
// host's volume mounts and keeps one verdict; if it kept whichever came first, a host that both
// gained a new volumeClaimTemplate and lost an existing PVC in the same reconcile could report
// "new" and skip data recovery entirely.
func TestMoreSevereKeepsDataLossOverVolumeAdded(t *testing.T) {
	require.Equal(t, ErrPVCIsLost, moreSevere(ErrPVCIsNew, ErrPVCIsLost), "loss must outrank added")
	require.Equal(t, ErrPVCIsLost, moreSevere(ErrPVCIsLost, ErrPVCIsNew), "order of arrival must not matter")
	require.Equal(t, ErrPVCWithLostPVDeleted, moreSevere(ErrPVCIsNew, ErrPVCWithLostPVDeleted))
	require.Equal(t, ErrPVCIsMissed, moreSevere(ErrPVCIsNew, ErrPVCIsMissed), "missed must outrank added")
	require.Equal(t, ErrPVCIsLost, moreSevere(ErrPVCIsMissed, ErrPVCIsLost), "loss must outrank missed")
}

func TestMoreSevereHandlesNil(t *testing.T) {
	require.Nil(t, moreSevere(nil, nil))
	require.Equal(t, ErrPVCIsNew, moreSevere(nil, ErrPVCIsNew))
	require.Equal(t, ErrPVCIsNew, moreSevere(ErrPVCIsNew, nil), "a later clean volume must not clear a verdict")
}

// ErrPVCIsNew must stay OUT of the data-loss classifier: routing it there would force a StatefulSet
// recreate WITH a ZK replica drop and a full DDL replay, which is the destructive busywork this
// change exists to stop.
func TestVolumeAddedIsNotDataLoss(t *testing.T) {
	require.True(t, ErrIsVolumeAdded(ErrPVCIsNew))
	require.False(t, ErrIsDataLoss(ErrPVCIsNew), "a newly added volume is not data loss")
	require.False(t, ErrIsVolumeMissed(ErrPVCIsNew))

	// and the existing verdicts must not have been reclassified
	require.True(t, ErrIsDataLoss(ErrPVCIsLost))
	require.True(t, ErrIsDataLoss(ErrPVCWithLostPVDeleted))
	require.True(t, ErrIsVolumeMissed(ErrPVCIsMissed))
	require.False(t, ErrIsVolumeAdded(ErrPVCIsLost))
	require.False(t, ErrIsVolumeAdded(ErrPVCIsMissed))
}

// An unrecognised verdict must rank above the benign ones, so a sentinel added later cannot be
// silently swallowed by an "added" verdict.
func TestUnknownVerdictOutranksVolumeAdded(t *testing.T) {
	unknown := ErrorDataPersistence(errors.New("some future pvc verdict"))
	require.Equal(t, unknown, moreSevere(ErrPVCIsNew, unknown))
}

// THE FAIL-SAFE. isNewVolume must answer "not new" when it cannot consult the ancestor, so an
// unknown volume is classified as LOST and recovery still runs. Getting this backwards is the
// destructive direction: the operator tolerates a failed status-ConfigMap read and the first
// reconcile after an upgrade may have no ancestor at all, so "no ancestor" is a reachable state -
// and answering "new" there would silently skip recovery for a volume that really was lost.
func TestIsNewVolumeFailsSafeWithoutAncestor(t *testing.T) {
	w := &Reconciler{}
	host := &api.Host{} // no CR, hence no ancestor
	mount := &core.VolumeMount{Name: "data-volume"}

	require.False(t, w.isNewVolume(host, mount),
		"without an ancestor the volume must NOT be treated as new - it must fall through to lost")
}
