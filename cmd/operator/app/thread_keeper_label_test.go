package app

import (
	"testing"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop/choptest"
)

const testShardKey = choptest.ShardLabelKey

var setWatchLabelSelector = choptest.SetWatchLabelSelector

func newLabeledCHK(labels map[string]string) *api.ClickHouseKeeperInstallation {
	return &api.ClickHouseKeeperInstallation{
		ObjectMeta: meta.ObjectMeta{
			Namespace: "clickhouse",
			Name:      "test-chk",
			Labels:    labels,
		},
	}
}

func Test_keeperPredicateWithLabelSelector(t *testing.T) {
	tests := []struct {
		name     string
		selector string
		labels   map[string]string
		want     bool
	}{
		{"shard operator passes matching CHK", testShardKey + "=stg", map[string]string{testShardKey: "stg"}, true},
		{"shard operator filters other shard's CHK", testShardKey + "=stg", map[string]string{testShardKey: "logs"}, false},
		{"shard operator filters unlabeled CHK", testShardKey + "=stg", nil, false},
		{"legacy operator passes unlabeled CHK", "!" + testShardKey, nil, true},
		{"legacy operator filters shard-labeled CHK", "!" + testShardKey, map[string]string{testShardKey: "stg"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setWatchLabelSelector(t, tt.selector)
			predicate := keeperPredicate()
			chk := newLabeledCHK(tt.labels)

			if got := predicate.Create(event.CreateEvent{Object: chk}); got != tt.want {
				t.Errorf("keeperPredicate.Create() = %v, want %v", got, tt.want)
			}
			if got := predicate.Update(event.UpdateEvent{ObjectNew: chk}); got != tt.want {
				t.Errorf("keeperPredicate.Update() = %v, want %v", got, tt.want)
			}
		})
	}
}

// A label flip arrives at both operators as a plain Update: the losing operator filters it
// (no delete flow), the gaining operator processes it as a normal reconcile.
func Test_keeperLabelFlipIsNotDelete(t *testing.T) {
	oldCHK := newLabeledCHK(nil)
	newCHK := newLabeledCHK(map[string]string{testShardKey: "stg"})

	t.Run("losing operator filters the flip update", func(t *testing.T) {
		setWatchLabelSelector(t, "!"+testShardKey)
		if keeperPredicate().Update(event.UpdateEvent{ObjectOld: oldCHK, ObjectNew: newCHK}) {
			t.Error("operator losing a CHK on label flip must filter the update")
		}
	})

	t.Run("gaining operator processes the flip update", func(t *testing.T) {
		setWatchLabelSelector(t, testShardKey+"=stg")
		if !keeperPredicate().Update(event.UpdateEvent{ObjectOld: oldCHK, ObjectNew: newCHK}) {
			t.Error("operator gaining a CHK on label flip must process the update")
		}
	})
}
