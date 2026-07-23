package v1

import (
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

func TestReconcileHostWaitReplicasSyncNormalizeDefaults(t *testing.T) {
	var syncConfig *ReconcileHostWaitReplicasSync
	syncConfig = syncConfig.Normalize()
	if syncConfig.IsEnabled() {
		t.Fatalf("enabled must default to false")
	}
	if syncConfig.GetMode() != "lightweight" {
		t.Fatalf("mode default = %q, want lightweight", syncConfig.GetMode())
	}
	if syncConfig.GetTimeout() != 0 {
		t.Fatalf("timeout default = %d, want 0 (unbounded)", syncConfig.GetTimeout())
	}
	if syncConfig.GetOnTimeout() != "abort" {
		t.Fatalf("onTimeout default = %q, want abort", syncConfig.GetOnTimeout())
	}
	if syncConfig.GetPollInterval() != 10 || syncConfig.GetSuccessThreshold() != 6 {
		t.Fatalf("health defaults = %d/%d, want 10/6", syncConfig.GetPollInterval(), syncConfig.GetSuccessThreshold())
	}
}

func TestReconcileHostWaitReplicasSyncNormalizeRejectsInvalid(t *testing.T) {
	syncConfig := &ReconcileHostWaitReplicasSync{
		Mode:      types.NewString("bogus"),
		Timeout:   types.NewInt32(-5),
		OnTimeout: types.NewString("explode"),
		Health: &ReconcileHostWaitReplicasSyncHealth{
			PollInterval:     types.NewInt32(0),
			SuccessThreshold: types.NewInt32(-1),
		},
	}
	syncConfig = syncConfig.Normalize()
	if syncConfig.GetMode() != "lightweight" || syncConfig.GetOnTimeout() != "abort" {
		t.Fatalf("invalid enums must fall back to defaults")
	}
	if syncConfig.GetTimeout() != 0 || syncConfig.GetPollInterval() != 10 || syncConfig.GetSuccessThreshold() != 6 {
		t.Fatalf("invalid numerics must fall back to defaults")
	}
}

func TestReconcileHostWaitReplicasSyncMergeFromPrefersLocal(t *testing.T) {
	localSyncConfig := (&ReconcileHostWaitReplicasSync{Enabled: types.NewStringBool(true)}).Normalize()
	parentSyncConfig := (&ReconcileHostWaitReplicasSync{Enabled: types.NewStringBool(false), Timeout: types.NewInt32(30)}).Normalize()
	mergedSyncConfig := localSyncConfig.MergeFrom(parentSyncConfig)
	if !mergedSyncConfig.IsEnabled() {
		t.Fatalf("merge must prefer local enabled=true")
	}
}
