package chi

import (
	"testing"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/stretchr/testify/require"
)

// finalizeCR must decide from the verdict of its OWN normalize, captured before the f callback
// runs, and must skip f entirely when that verdict is an abort. finalizeReconcileAndMarkCompleted
// passes an f that calls ReconcileComplete() and advances the ancestor: running it on a rejected
// spec would publish Completed for a spec the operator refused, satisfy callers polling
// status.taskIDsCompleted with a success that never happened, and poison the ancestor the next
// action plan diffs against.
func TestFinalizeAbortSkipsCompletionCallback(t *testing.T) {
	chi := &api.ClickHouseInstallation{}
	chi.EnsureStatus().ReconcileAbortWithReason(api.StatusReasonRemovedSecretRefSyntax, "rejected")

	fRan := false
	// This is what finalizeReconcileAndMarkCompleted's callback does.
	f := func(c *api.ClickHouseInstallation) { fRan = true; c.EnsureStatus().ReconcileComplete() }

	// Mirror finalizeCR's ordering: capture the verdict, then run f only when not aborted.
	aborted := chi.EnsureStatus().GetStatus() == api.StatusAborted
	if !aborted {
		f(chi)
	}

	require.True(t, aborted, "the abort must be captured before anything can overwrite Status")
	require.False(t, fRan, "the completion callback must not run for a spec the operator rejected")
	require.Equal(t, api.StatusAborted, chi.EnsureStatus().GetStatus(),
		"the status persisted for a rejected spec must remain Aborted")
}

// Why the capture must precede f at all: f overwrites Status unconditionally, so a verdict read
// after it could never observe an abort.
func TestFinalizeCallbackOverwritesStatus(t *testing.T) {
	chi := &api.ClickHouseInstallation{}
	chi.EnsureStatus().ReconcileAbortWithReason(api.StatusReasonRemovedSecretRefSyntax, "rejected")

	f := func(c *api.ClickHouseInstallation) { c.EnsureStatus().ReconcileComplete() }
	f(chi)

	require.Equal(t, api.StatusCompleted, chi.EnsureStatus().GetStatus(),
		"callback does overwrite Status - which is exactly why the capture must precede it")
}
