package clickhouse

import (
	"testing"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop/choptest"
)

const testShardKey = choptest.ShardLabelKey

func init() {
	// shouldWatchCR() → chop.Config(), so the global chop singleton must be initialized.
	choptest.EnsureInit()
}

var setWatchLabelSelector = choptest.SetWatchLabelSelector

func newDiscoveredCHI(labels map[string]string) *api.ClickHouseInstallation {
	return &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{
			Namespace: "clickhouse",
			Name:      "test-chi",
			Labels:    labels,
		},
	}
}

func Test_shouldWatchCRWithLabelSelector(t *testing.T) {
	e := &Exporter{}

	tests := []struct {
		name     string
		selector string
		labels   map[string]string
		want     bool
	}{
		{"no selector watches everything (backward compat)", "", map[string]string{testShardKey: "logs"}, true},
		{"no selector watches unlabeled", "", nil, true},
		{"shard exporter watches matching CHI", testShardKey + "=stg", map[string]string{testShardKey: "stg"}, true},
		{"shard exporter skips other shard's CHI", testShardKey + "=stg", map[string]string{testShardKey: "logs"}, false},
		{"shard exporter skips unlabeled CHI", testShardKey + "=stg", nil, false},
		{"legacy exporter watches unlabeled CHI", "!" + testShardKey, nil, true},
		{"legacy exporter skips shard-labeled CHI", "!" + testShardKey, map[string]string{testShardKey: "stg"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.selector != "" {
				setWatchLabelSelector(t, tt.selector)
			}
			if got := e.shouldWatchCR(newDiscoveredCHI(tt.labels)); got != tt.want {
				t.Errorf("shouldWatchCR() = %v, want %v", got, tt.want)
			}
		})
	}
}

// A stopped CHI is never watched, selector or not.
func Test_shouldWatchCRStoppedStillSkipped(t *testing.T) {
	e := &Exporter{}
	setWatchLabelSelector(t, testShardKey+"=stg")

	chi := newDiscoveredCHI(map[string]string{testShardKey: "stg"})
	chi.Spec.Stop = types.NewStringBool(true)
	if e.shouldWatchCR(chi) {
		t.Error("stopped CHI watched, want skipped")
	}
}
