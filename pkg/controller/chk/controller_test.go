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

package chk

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
)

// TestReconcileResult pins the deferred -> soft-requeue mapping.
//
// ErrCRUDDeferred means "postponed", not "failed": returning it as an error would hand the
// request to controller-runtime's exponential backoff, so a quorum wait that clears in
// seconds would be retried minutes later. It must come back as a fixed RequeueAfter with a
// nil error instead. Every other error keeps the normal error-return (backoff) behaviour.
func TestReconcileResult(t *testing.T) {
	c := &Controller{}
	cr := &apiChk.ClickHouseKeeperInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: "test", Name: "keeper"},
	}

	t.Run("nil error requeues nothing", func(t *testing.T) {
		res, err := c.reconcileResult(cr, nil)
		require.NoError(t, err)
		require.Equal(t, ctrl.Result{}, res)
	})

	t.Run("wrapped deferred is a soft requeue, not an error", func(t *testing.T) {
		// The reconciler wraps the sentinel with host context on its way up, so the
		// mapping must match via errors.Is - an == comparison would miss this.
		err := fmt.Errorf("host h0: %w", common.ErrCRUDDeferred)
		require.NotEqual(t, common.ErrCRUDDeferred, err, "wrapped error must not be the bare sentinel")
		require.True(t, errors.Is(err, common.ErrCRUDDeferred))

		res, resErr := c.reconcileResult(cr, err)
		require.NoError(t, resErr, "deferred must not reach controller-runtime as an error (exp backoff)")
		require.Equal(t, raftQuorumDeferredRequeueAfter, res.RequeueAfter)
		require.Equal(t, ctrl.Result{RequeueAfter: raftQuorumDeferredRequeueAfter}, res)
	})

	t.Run("bare deferred sentinel is a soft requeue", func(t *testing.T) {
		res, err := c.reconcileResult(cr, common.ErrCRUDDeferred)
		require.NoError(t, err)
		require.Equal(t, raftQuorumDeferredRequeueAfter, res.RequeueAfter)
	})

	t.Run("abort is a hard error with no requeue", func(t *testing.T) {
		res, err := c.reconcileResult(cr, common.ErrCRUDAbort)
		require.ErrorIs(t, err, common.ErrCRUDAbort)
		require.Equal(t, ctrl.Result{}, res, "hard errors must rely on error backoff, not RequeueAfter")
		require.Zero(t, res.RequeueAfter)
	})

	t.Run("terminal staged startup abort is not requeued", func(t *testing.T) {
		err := fmt.Errorf("%w: %w",
			common.ErrCRUDAbort, errRaftStagedJoinerStartupTimeout)
		res, outErr := c.reconcileResult(cr, err)
		require.NoError(t, outErr,
			"terminal startup abort was already persisted as Aborted, so runtime must not retry it")
		require.Equal(t, ctrl.Result{}, res)
	})

	t.Run("arbitrary error is returned verbatim", func(t *testing.T) {
		boom := errors.New("boom")
		res, err := c.reconcileResult(cr, boom)
		require.Same(t, boom, err)
		require.Equal(t, ctrl.Result{}, res)
	})

	t.Run("nil CR does not panic on the defer log path", func(t *testing.T) {
		res, err := c.reconcileResult(nil, common.ErrCRUDDeferred)
		require.NoError(t, err)
		require.Equal(t, raftQuorumDeferredRequeueAfter, res.RequeueAfter)
	})
}

// TestHoldRaftStagedStartupAbort ensures status and child-object watch events
// cannot turn a persisted terminal abort back into InProgress. Only a spec
// edit (new Kubernetes generation) should resume an aborted image pull.
func TestHoldRaftStagedStartupAbort(t *testing.T) {
	target := &apiChk.ClickHouseKeeperInstallation{
		ObjectMeta: meta.ObjectMeta{Generation: 7},
	}
	cr := &apiChk.ClickHouseKeeperInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: "test", Name: "keeper", Generation: 7},
		Status: &apiChk.Status{
			Status:       apiChk.StatusAborted,
			NormalizedCR: target,
			Errors:       []string{raftStagedJoinerStartupTimeoutStatusMarker + " failed to start"},
		},
	}

	t.Run("same spec generation stays aborted after resync", func(t *testing.T) {
		require.True(t, shouldHoldRaftStagedStartupAbort(cr))
	})
	t.Run("spec edit permits image recovery", func(t *testing.T) {
		updated := cr.DeepCopy()
		updated.Generation = 8
		require.False(t, shouldHoldRaftStagedStartupAbort(updated))
	})
	t.Run("unrelated abort retains legacy behavior", func(t *testing.T) {
		updated := cr.DeepCopy()
		updated.Status.Errors = []string{"unrelated error"}
		require.False(t, shouldHoldRaftStagedStartupAbort(updated))
	})
	t.Run("in progress is never held", func(t *testing.T) {
		updated := cr.DeepCopy()
		updated.Status.Status = apiChk.StatusInProgress
		require.False(t, shouldHoldRaftStagedStartupAbort(updated))
	})
	t.Run("missing last-attempt generation is not a permanent lock", func(t *testing.T) {
		updated := cr.DeepCopy()
		updated.Status.NormalizedCR = nil
		require.False(t, shouldHoldRaftStagedStartupAbort(updated))
	})
	t.Run("legacy zero generation is not a permanent lock", func(t *testing.T) {
		updated := cr.DeepCopy()
		updated.Status.NormalizedCR.Generation = 0
		require.False(t, shouldHoldRaftStagedStartupAbort(updated))
	})
}
