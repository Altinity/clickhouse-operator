package chi

import (
	"testing"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopFake "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned/fake"
)

func newKeeperReferencingCHI(name string, labels map[string]string, chkName string) *api.ClickHouseInstallation {
	return &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{
			Namespace: "clickhouse",
			Name:      name,
			Labels:    labels,
		},
		Spec: api.ChiSpec{
			Configuration: &api.Configuration{
				Zookeeper: &api.ZookeeperConfig{
					Keeper: &api.KeeperRef{Name: chkName},
				},
			},
		},
	}
}

func queuedItems(c *Controller) int {
	total := 0
	for i := range c.queues {
		total += c.queues[i].Len()
	}
	return total
}

// enqueueDependentCHIs must only enqueue CHIs within this operator's watch scope. The
// triggering CHK is not label-filtered: a CHI of this shard may reference another shard's CHK.
func Test_enqueueDependentCHIsWithLabelSelector(t *testing.T) {
	stgCHI := newKeeperReferencingCHI("chi-stg", map[string]string{testShardKey: "stg"}, "keeper1")
	logsCHI := newKeeperReferencingCHI("chi-logs", map[string]string{testShardKey: "logs"}, "keeper1")
	unrelatedCHI := newKeeperReferencingCHI("chi-other-keeper", map[string]string{testShardKey: "stg"}, "keeper2")

	newController := func() *Controller {
		c := &Controller{
			chopClient: chopFake.NewSimpleClientset(stgCHI, logsCHI, unrelatedCHI),
		}
		c.initQueues()
		return c
	}

	t.Run("no selector enqueues all CHIs referencing the CHK (backward compat)", func(t *testing.T) {
		c := newController()
		c.enqueueDependentCHIs("clickhouse", "keeper1")
		if got := queuedItems(c); got != 2 {
			t.Errorf("queued %d CHIs, want 2 (both keeper1 referents, any labels)", got)
		}
	})

	t.Run("shard selector enqueues only matching CHIs", func(t *testing.T) {
		setWatchLabelSelector(t, testShardKey+"=stg")
		c := newController()
		c.enqueueDependentCHIs("clickhouse", "keeper1")
		if got := queuedItems(c); got != 1 {
			t.Errorf("queued %d CHIs, want 1 (only the stg-labeled keeper1 referent)", got)
		}
	})

	t.Run("legacy selector enqueues nothing when all referents are shard-labeled", func(t *testing.T) {
		setWatchLabelSelector(t, "!"+testShardKey)
		c := newController()
		c.enqueueDependentCHIs("clickhouse", "keeper1")
		if got := queuedItems(c); got != 0 {
			t.Errorf("queued %d CHIs, want 0", got)
		}
	})
}
