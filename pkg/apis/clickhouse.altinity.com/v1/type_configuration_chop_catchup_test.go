package v1

import (
	"strings"
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

func TestReconcileHostWaitReplicasCatchUpNormalizeDefaults(t *testing.T) {
	var catchUpConfig *ReconcileHostWaitReplicasCatchUp
	catchUpConfig = catchUpConfig.Normalize()
	if catchUpConfig.IsEnabled() {
		t.Fatalf("enabled must default to false")
	}
	if catchUpConfig.GetTimeout() != 900 {
		t.Fatalf("timeout default = %d, want 900", catchUpConfig.GetTimeout())
	}
	if !strings.EqualFold(catchUpConfig.GetOnTimeout(), CatchUpOnTimeoutAbort) {
		t.Fatalf("onTimeout default = %q, want %q", catchUpConfig.GetOnTimeout(), CatchUpOnTimeoutAbort)
	}
	if catchUpConfig.GetPollInterval() != 10 || catchUpConfig.GetSuccessThreshold() != 6 {
		t.Fatalf("health defaults = %d/%d, want 10/6", catchUpConfig.GetPollInterval(), catchUpConfig.GetSuccessThreshold())
	}
}

func TestReconcileHostWaitReplicasCatchUpNormalizeRejectsInvalid(t *testing.T) {
	catchUpConfig := &ReconcileHostWaitReplicasCatchUp{
		Timeout:   types.NewInt32(-5),
		OnTimeout: types.NewString("explode"),
		Health: &ReconcileHostWaitReplicasCatchUpHealth{
			PollInterval:     types.NewInt32(0),
			SuccessThreshold: types.NewInt32(-1),
		},
	}
	catchUpConfig = catchUpConfig.Normalize()
	if !strings.EqualFold(catchUpConfig.GetOnTimeout(), CatchUpOnTimeoutAbort) {
		t.Fatalf("invalid enums must fall back to defaults")
	}
	if catchUpConfig.GetTimeout() != 900 || catchUpConfig.GetPollInterval() != 10 || catchUpConfig.GetSuccessThreshold() != 6 {
		t.Fatalf("invalid numerics must fall back to defaults")
	}
}

func TestReconcileHostWaitReplicasCatchUpMergeFromPrefersLocal(t *testing.T) {
	localSyncConfig := (&ReconcileHostWaitReplicasCatchUp{Enabled: types.NewStringBool(true)}).Normalize()
	parentSyncConfig := (&ReconcileHostWaitReplicasCatchUp{Enabled: types.NewStringBool(false), Timeout: types.NewInt32(30)}).Normalize()
	mergedSyncConfig := localSyncConfig.MergeFrom(parentSyncConfig)
	if !mergedSyncConfig.IsEnabled() {
		t.Fatalf("merge must prefer local enabled=true")
	}
}

// Normalize must keep whatever case it is given rather than discarding it as invalid and
// silently reverting to the default. The CRD advertises the two canonical spellings, so those
// are what an API user can set; the config-file path is not schema-checked, hence the
// arbitrary-case entry below.
func TestReconcileHostWaitReplicasCatchUpOnTimeoutAcceptsEitherCase(t *testing.T) {
	for _, onTimeout := range []string{"abort", "Abort", "proceed", "Proceed", "PROCEED"} {
		catchUpConfig := (&ReconcileHostWaitReplicasCatchUp{OnTimeout: types.NewString(onTimeout)}).Normalize()
		if catchUpConfig.GetOnTimeout() != onTimeout {
			t.Fatalf("onTimeout %q was not accepted, got %q", onTimeout, catchUpConfig.GetOnTimeout())
		}
	}
}
