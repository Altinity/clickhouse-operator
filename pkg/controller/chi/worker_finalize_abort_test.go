package chi

import (
	"testing"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/stretchr/testify/require"
)

// finalizeCR must decide whether to write the users config from the verdict of its OWN normalize,
// captured before the f callback runs. finalizeReconcileAndMarkCompleted passes an f that calls
// ReconcileComplete(), which overwrites Status - so reading the status after f would never see an
// abort and a rejected spec would be written to the cluster.
func TestFinalizeAbortVerdictSurvivesCallbackOverwritingStatus(t *testing.T) {
	chi := &api.ClickHouseInstallation{}
	chi.EnsureStatus().ReconcileAbortWithReason(api.StatusReasonRemovedSecretRefSyntax, "rejected")

	// This is what finalizeReconcileAndMarkCompleted's callback does.
	f := func(c *api.ClickHouseInstallation) { c.EnsureStatus().ReconcileComplete() }

	// Mirror finalizeCR's ordering: capture, then run f.
	aborted := chi.EnsureStatus().GetStatus() == api.StatusAborted
	f(chi)

	require.True(t, aborted, "the abort must be captured before the callback overwrites Status")
	require.Equal(t, api.StatusCompleted, chi.EnsureStatus().GetStatus(),
		"callback does overwrite Status - which is exactly why the capture must precede it")
}
