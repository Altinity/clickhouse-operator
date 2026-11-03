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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
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

// stubPVC implements just enough of IKubeStoragePVC to drive deletePVC. The embedded nil interface
// means any method the test does not expect panics, which keeps the stub honest.
type stubPVC struct {
	interfaces.IKubeStoragePVC
	getReturn   *core.PersistentVolumeClaim
	getErr      error
	getCalls    int
	deleteCalls int
	updateCalls int
	// onGet fires on each Get, so a test can cancel the context from inside the poll.
	onGet func()
}

func (s *stubPVC) Get(context.Context, string, string) (*core.PersistentVolumeClaim, error) {
	s.getCalls++
	if s.onGet != nil {
		s.onGet()
	}
	if s.getErr == nil {
		return s.getReturn, nil
	}
	// nil object with the error, as the CHK adapter does (chk/kube/pvc.go returns nil from the
	// GetWithRetry closure). The CHI adapter hands back client-go's non-nil zero object instead,
	// which is why the dereference this guards was only ever reachable on the Keeper path.
	return nil, s.getErr
}

func (s *stubPVC) Delete(context.Context, string, string) error { s.deleteCalls++; return nil }

func (s *stubPVC) UpdateOrCreate(_ context.Context, pvc *core.PersistentVolumeClaim) (*core.PersistentVolumeClaim, error) {
	s.updateCalls++
	return pvc, nil
}

// TestDeletePVCSurvivesUnreadablePVC pins the in-loop guard against an unreadable PVC.
//
// deletePVC polls until the PVC is gone. A cached read essentially only ever answered NotFound, so
// the non-NotFound branch fell through to curPVC.Finalizers on a nil object. Keeper reads now go
// straight to the API server, where Forbidden and a spent retry budget both reach this
// loop - and the Keeper adapter returns a nil object with the error, so that fall-through became a
// live nil dereference that crashes the operator instead of retrying.
//
// The stub cancels from inside the read purely to stop the hour-long poll; the guard itself does
// not depend on the context, and deletePVC has no pre-loop cancellation check by design.
func TestDeletePVCSurvivesUnreadablePVC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pvcAPI := &stubPVC{
		getErr: apiErrors.NewForbidden(
			schema.GroupResource{Resource: "persistentvolumeclaims"}, "pvc", errors.New("nope")),
		onGet: cancel,
	}
	w := &Reconciler{pvc: pvcAPI}

	pvc := &core.PersistentVolumeClaim{}
	pvc.Namespace, pvc.Name = "ns", "pvc"

	var deleted bool
	require.NotPanics(t, func() { deleted = w.deletePVC(ctx, pvc) })

	require.False(t, deleted, "the PVC was never confirmed gone, so this must not report success")
	require.Equal(t, 1, pvcAPI.deleteCalls, "the Delete must still be issued - the caller reports it as done")
	require.Equal(t, 1, pvcAPI.getCalls, "the poll must stop once the context is cancelled, not run for an hour")
}

// The success path has its own wait, and it is a separate mutation target: reverting only that one
// to a blind time.Sleep left the suite green while re-introducing a poll that ignores shutdown.
// Here the PVC is readable and still present - the normal "waiting for it to go away" case.
func TestDeletePVCStopsPollingPresentPVCOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	present := &core.PersistentVolumeClaim{}
	present.Namespace, present.Name = "ns", "pvc"
	present.Finalizers = []string{"kubernetes.io/pvc-protection"}

	pvcAPI := &stubPVC{getReturn: present, onGet: cancel}
	w := &Reconciler{pvc: pvcAPI}

	deleted := w.deletePVC(ctx, present)

	require.False(t, deleted, "a PVC still present when shutdown arrives was not deleted")
	require.Equal(t, 1, pvcAPI.getCalls, "the poll must honour cancellation on the success path too")
	require.Equal(t, 1, pvcAPI.updateCalls, "a lingering finalizer must be cleared, or the PVC never goes away")
}
