package chi

import (
	"testing"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop/choptest"
	chopListers "github.com/altinity/clickhouse-operator/pkg/client/listers/clickhouse.altinity.com/v1"
)

const (
	testShardKey = choptest.ShardLabelKey

	// CHOP-generated child object labels (see pkg/model/chi/tags/labeler/list.go)
	labelApp    = "clickhouse.altinity.com/app"
	labelAppVal = "chop"
	labelCRName = "clickhouse.altinity.com/chi"
)

var setWatchLabelSelector = choptest.SetWatchLabelSelector

func newCHI(name string, labels map[string]string) *api.ClickHouseInstallation {
	return &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{
			Namespace: "clickhouse",
			Name:      name,
			Labels:    labels,
		},
	}
}

func Test_shouldEnqueueWithLabelSelector(t *testing.T) {
	tests := []struct {
		name     string
		selector string
		labels   map[string]string
		want     bool
	}{
		{"shard operator enqueues matching CHI", testShardKey + "=stg", map[string]string{testShardKey: "stg"}, true},
		{"shard operator skips other shard's CHI", testShardKey + "=stg", map[string]string{testShardKey: "logs"}, false},
		{"shard operator skips unlabeled CHI", testShardKey + "=stg", nil, false},
		{"shard operator skips CHI with unrelated labels", testShardKey + "=stg", map[string]string{"unrelated": "value"}, false},
		{"legacy operator enqueues unlabeled CHI", "!" + testShardKey, nil, true},
		{"legacy operator enqueues CHI with unrelated labels", "!" + testShardKey, map[string]string{"unrelated": "value"}, true},
		{"legacy operator skips shard-labeled CHI", "!" + testShardKey, map[string]string{testShardKey: "stg"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setWatchLabelSelector(t, tt.selector)
			if got := ShouldEnqueue(newCHI("test-chi", tt.labels)); got != tt.want {
				t.Errorf("ShouldEnqueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// A label flip arrives at both operators as a plain Update: the losing operator ignores it
// (no delete flow), the gaining operator reconciles it.
func Test_labelFlipIsNotDelete(t *testing.T) {
	flipped := newCHI("flipping-chi", map[string]string{testShardKey: "stg"})

	t.Run("losing operator ignores the update and would ignore a delete", func(t *testing.T) {
		setWatchLabelSelector(t, "!"+testShardKey)
		if ShouldEnqueue(flipped) {
			t.Error("operator losing a CHI on label flip must not enqueue any work for it")
		}
	})

	t.Run("gaining operator sees a normal reconcile", func(t *testing.T) {
		setWatchLabelSelector(t, testShardKey+"=stg")
		if !ShouldEnqueue(flipped) {
			t.Error("operator gaining a CHI on label flip must enqueue a normal reconcile")
		}
	})
}

// A shard flip while a command sits in the queue must be caught at dequeue time: ownsCR
// re-checks live labels so the losing operator drops the stale work instead of reconciling
// (and purging) a CHI the gaining operator now owns.
func Test_ownsCRAfterLabelFlip(t *testing.T) {
	setWatchLabelSelector(t, testShardKey+"=stg")

	if !ownsCR(newCHI("chi", map[string]string{testShardKey: "stg"})) {
		t.Error("ownsCR() = false for in-shard CHI, want true")
	}
	// Live labels flipped to another shard after enqueue
	if ownsCR(newCHI("chi", map[string]string{testShardKey: "logs"})) {
		t.Error("ownsCR() = true for flipped CHI, want false (stale queued work must be dropped)")
	}
	if ownsCR(newCHI("chi", nil)) {
		t.Error("ownsCR() = true for unlabeled CHI under shard selector, want false")
	}
}

func Test_isFlipAway(t *testing.T) {
	setWatchLabelSelector(t, testShardKey+"=stg")
	stg := newCHI("chi", map[string]string{testShardKey: "stg"})
	logs := newCHI("chi", map[string]string{testShardKey: "logs"})

	tests := []struct {
		name     string
		old, new *api.ClickHouseInstallation
		want     bool
	}{
		{"flip away triggers local cleanup", stg, logs, true},
		{"still owned", stg, stg, false},
		{"never owned", logs, logs, false},
		{"flip toward is a normal enqueue", logs, stg, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isFlipAway(tt.old, tt.new); got != tt.want {
				t.Errorf("isFlipAway() = %v, want %v", got, tt.want)
			}
		})
	}
}

// newTestControllerWithCHIs builds a Controller whose CHI lister is backed by an in-memory
// cache containing the given CHIs.
func newTestControllerWithCHIs(t *testing.T, chis ...*api.ClickHouseInstallation) *Controller {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, chi := range chis {
		if err := indexer.Add(chi); err != nil {
			t.Fatalf("failed to add CHI to test indexer: %v", err)
		}
	}
	return &Controller{
		chiLister: chopListers.NewClickHouseInstallationLister(indexer),
	}
}

// newChildObject builds a CHOP-generated child object owned by the named CHI. Child objects
// carry no shard label — the selector is resolved against the owning CHI.
func newChildObject(owningCHI string) *core.ConfigMap {
	return &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Namespace: "clickhouse",
			Name:      "child-of-" + owningCHI,
			Labels: map[string]string{
				labelApp:    labelAppVal,
				labelCRName: owningCHI,
			},
		},
	}
}

func Test_isTrackedObjectWithLabelSelector(t *testing.T) {
	stgCHI := newCHI("chi-stg", map[string]string{testShardKey: "stg"})
	logsCHI := newCHI("chi-logs", map[string]string{testShardKey: "logs"})
	unlabeledCHI := newCHI("chi-unlabeled", nil)
	c := newTestControllerWithCHIs(t, stgCHI, logsCHI, unlabeledCHI)

	t.Run("no selector: all CHOP-generated objects tracked (backward compat)", func(t *testing.T) {
		for _, owner := range []string{"chi-stg", "chi-logs", "chi-unlabeled", "chi-not-in-cache"} {
			if !c.isTrackedObject(&newChildObject(owner).ObjectMeta) {
				t.Errorf("child of %q not tracked without selector, want tracked", owner)
			}
		}
	})

	t.Run("no selector: non-CHOP object still untracked", func(t *testing.T) {
		plain := &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: "clickhouse", Name: "plain"}}
		if c.isTrackedObject(&plain.ObjectMeta) {
			t.Error("non-CHOP-generated object tracked, want untracked")
		}
	})

	t.Run("shard selector: tracked iff owning CHI matches", func(t *testing.T) {
		setWatchLabelSelector(t, testShardKey+"=stg")
		if !c.isTrackedObject(&newChildObject("chi-stg").ObjectMeta) {
			t.Error("child of matching CHI untracked, want tracked")
		}
		if c.isTrackedObject(&newChildObject("chi-logs").ObjectMeta) {
			t.Error("child of other shard's CHI tracked, want untracked")
		}
		if c.isTrackedObject(&newChildObject("chi-unlabeled").ObjectMeta) {
			t.Error("child of unlabeled CHI tracked under shard selector, want untracked")
		}
	})

	t.Run("legacy selector: tracked iff owning CHI is unlabeled", func(t *testing.T) {
		setWatchLabelSelector(t, "!"+testShardKey)
		if !c.isTrackedObject(&newChildObject("chi-unlabeled").ObjectMeta) {
			t.Error("child of unlabeled CHI untracked under legacy selector, want tracked")
		}
		if c.isTrackedObject(&newChildObject("chi-stg").ObjectMeta) {
			t.Error("child of shard-labeled CHI tracked under legacy selector, want untracked")
		}
	})

	t.Run("selector set: owning CHI missing from cache => untracked, no panic", func(t *testing.T) {
		setWatchLabelSelector(t, testShardKey+"=stg")
		if c.isTrackedObject(&newChildObject("chi-not-in-cache").ObjectMeta) {
			t.Error("child with unresolvable owning CHI tracked, want untracked (cannot attribute to a shard)")
		}
	})

	t.Run("selector set: CHOP object without CR name label => untracked, no panic", func(t *testing.T) {
		setWatchLabelSelector(t, testShardKey+"=stg")
		orphan := &core.ConfigMap{
			ObjectMeta: meta.ObjectMeta{
				Namespace: "clickhouse",
				Name:      "orphan",
				Labels:    map[string]string{labelApp: labelAppVal},
			},
		}
		if c.isTrackedObject(&orphan.ObjectMeta) {
			t.Error("CHOP object without CR name label tracked under selector, want untracked")
		}
	})

	t.Run("selector set: nil lister => untracked, no panic", func(t *testing.T) {
		setWatchLabelSelector(t, testShardKey+"=stg")
		noLister := &Controller{}
		if noLister.isTrackedObject(&newChildObject("chi-stg").ObjectMeta) {
			t.Error("tracked with nil lister under selector, want untracked")
		}
	})

	t.Run("selector set: CHI cache not yet synced => tracked (startup fallback)", func(t *testing.T) {
		// Before the initial CHI list completes, a cache miss means "not synced yet", not
		// "does not exist" — dropping edge-triggered child events (e.g. EndpointSlice IP
		// assignment) here would lose them permanently, since resync produces no diff.
		setWatchLabelSelector(t, testShardKey+"=stg")
		unsynced := newTestControllerWithCHIs(t) // empty cache
		unsynced.chiListerSynced = func() bool { return false }
		if !unsynced.isTrackedObject(&newChildObject("chi-not-listed-yet").ObjectMeta) {
			t.Error("child untracked while CHI cache not synced, want tracked (downstream guards re-check ownership)")
		}
		// Once synced, an actual cache miss means untracked again
		unsynced.chiListerSynced = func() bool { return true }
		if unsynced.isTrackedObject(&newChildObject("chi-not-listed-yet").ObjectMeta) {
			t.Error("child of missing CHI tracked after cache sync, want untracked")
		}
	})
}
