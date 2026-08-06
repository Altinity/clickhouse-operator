package chi

import (
	"errors"
	"testing"
	"time"

	common "github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
)

func healthWindowStepForTest(counter int, ok bool, threshold int) (int, bool) {
	return healthWindowStep(counter, ok, threshold)
}

func TestHealthWindowConsecutive(t *testing.T) {
	counter := 0
	done := false
	for i := 0; i < 6; i++ {
		counter, done = healthWindowStepForTest(counter, true, 6)
	}
	if !done || counter != 6 {
		t.Fatalf("6 consecutive OK must satisfy threshold; counter=%d done=%v", counter, done)
	}
}

func TestHealthWindowResetsOnFailure(t *testing.T) {
	counter, _ := healthWindowStepForTest(0, true, 6)
	counter, _ = healthWindowStepForTest(counter, true, 6)
	counter, done := healthWindowStepForTest(counter, false, 6)
	if counter != 0 || done {
		t.Fatalf("not-OK poll must reset counter; counter=%d done=%v", counter, done)
	}
}

func TestOnSoftTimeoutNeverPushesMarker(t *testing.T) {
	advance, pushMarker, err := onSoftTimeout("proceed")
	if !advance || pushMarker || err != nil {
		t.Fatalf("proceed => advance without marker; got advance=%v push=%v err=%v", advance, pushMarker, err)
	}

	advance, pushMarker, err = onSoftTimeout("abort")
	if advance || pushMarker || !errors.Is(err, common.ErrCRUDAbort) {
		t.Fatalf("abort => abort without marker; got advance=%v push=%v err=%v", advance, pushMarker, err)
	}
}

func TestSyncGateHealthStepTreatsHardFailAsNotReadyBeforeDeadline(t *testing.T) {
	counter, done, hardDeadline := syncGateHealthStep(3, true, true, 6, time.Second)
	if counter != 0 || done || hardDeadline {
		t.Fatalf("hard health before deadline must reset and keep waiting; counter=%d done=%v hardDeadline=%v", counter, done, hardDeadline)
	}
}

func TestSyncGateHealthStepReturnsHardFailAtDeadline(t *testing.T) {
	counter, done, hardDeadline := syncGateHealthStep(3, true, true, 6, 0)
	if counter != 0 || done || !hardDeadline {
		t.Fatalf("hard health at deadline must hard fail; counter=%d done=%v hardDeadline=%v", counter, done, hardDeadline)
	}
}

func TestReplicaSyncGateEventReasonDistinguishesProceedWithoutMarker(t *testing.T) {
	if got := replicaSyncGateEventReason(true); got != a.EventReasonReconcileCompleted {
		t.Fatalf("caught-up sync gate must report completed event; got %s", got)
	}
	if got := replicaSyncGateEventReason(false); got == a.EventReasonReconcileCompleted {
		t.Fatalf("proceed without marker must not report completed event")
	}
}
