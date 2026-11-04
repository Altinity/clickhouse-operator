package chi

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	commonTypes "github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop/choptest"
	chopListers "github.com/altinity/clickhouse-operator/pkg/client/listers/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/cmd_queue"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
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

func Test_isFlipToward(t *testing.T) {
	setWatchLabelSelector(t, testShardKey+"=stg")
	stg := newCHI("chi", map[string]string{testShardKey: "stg"})
	logs := newCHI("chi", map[string]string{testShardKey: "logs"})

	if !isFlipToward(logs, stg) {
		t.Error("flip toward not detected")
	}
	if isFlipToward(stg, logs) || isFlipToward(stg, stg) {
		t.Error("non-acquisition detected as flip toward")
	}
}

func Test_unwatchCancelsActiveReconcile(t *testing.T) {
	setWatchLabelSelector(t, testShardKey+"=stg")
	stg := newCHI("chi", map[string]string{testShardKey: "stg"})
	logs := newCHI("chi", map[string]string{testShardKey: "logs"})
	c := &Controller{}
	c.initQueues()
	c.enqueueObject(cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, stg))

	for _, q := range c.queues {
		if q.Len() == 0 {
			continue
		}
		item, ctx, ok := q.Get()
		if !ok {
			t.Fatal("failed to get queued reconcile")
		}
		c.enqueueObject(cmd_queue.NewReconcileCHI(cmd_queue.ReconcileUnwatch, stg, logs))
		select {
		case <-ctx.Done():
		default:
			t.Fatal("unwatch did not cancel the active reconcile")
		}
		q.Done(item)

		next, _, ok := q.Get()
		if !ok {
			t.Fatal("failed to get queued unwatch")
		}
		unwatch, ok := next.(*cmd_queue.ReconcileCHI)
		if !ok || unwatch.Cmd != cmd_queue.ReconcileUnwatch {
			t.Fatalf("next command = %#v, want unwatch", next)
		}
		q.Done(next)
		return
	}
	t.Fatal("reconcile was not queued")
}

func Test_watchedCRForHandoffUsesCompletedTopologyAndCurrentLabels(t *testing.T) {
	ancestor := newCHI("chi", map[string]string{testShardKey: "logs"})
	ancestor.Spec.Configuration = &api.Configuration{
		Clusters: []*api.Cluster{{
			Name:   "completed-topology",
			Layout: &api.ChiClusterLayout{},
		}},
	}
	current := newCHI("chi", map[string]string{testShardKey: "stg"})
	current.Annotations = map[string]string{"owner": "stg"}
	current.SetAncestor(ancestor)

	watched := watchedCRForHandoff(current)
	if got := watched.Labels[testShardKey]; got != "stg" {
		t.Errorf("watched shard label = %q, want stg", got)
	}
	if got := watched.Annotations["owner"]; got != "stg" {
		t.Errorf("watched owner annotation = %q, want stg", got)
	}
	if len(watched.Clusters) != 1 || watched.Clusters[0].Name != "completed-topology" {
		t.Errorf("watched clusters = %#v, want completed topology", watched.Clusters)
	}

	unreconciled := newCHI("new-chi", map[string]string{testShardKey: "stg"})
	unreconciled.Spec.Configuration = ancestor.Spec.Configuration
	if got := watchedCRForHandoff(unreconciled).Clusters; len(got) != 0 {
		t.Errorf("unreconciled watched clusters = %#v, want empty allocation", got)
	}
}

func Test_watchOperationsAreSerializedPerCHI(t *testing.T) {
	c := &Controller{}
	chi := newCHI("chi", map[string]string{testShardKey: "stg"})
	watched := watchedCRForHandoff(chi)
	started := make(chan struct{})
	release := make(chan struct{})
	results := make(chan string, 2)

	c.enqueueWatchOperation(watched, func() error {
		close(started)
		<-release
		results <- "first"
		return nil
	})
	<-started
	c.enqueueWatchOperation(watched, func() error {
		results <- "second"
		return nil
	})

	select {
	case got := <-results:
		t.Fatalf("watch operation %q completed before first was released", got)
	default:
	}
	close(release)
	for _, want := range []string{"first", "second"} {
		select {
		case got := <-results:
			if got != want {
				t.Fatalf("watch operation = %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for watch operation %q", want)
		}
	}
}

func Test_watchOperationRetriesFailure(t *testing.T) {
	c := &Controller{}
	watched := watchedCRForHandoff(newCHI("chi", map[string]string{testShardKey: "stg"}))
	completed := make(chan struct{})
	attempts := 0

	c.enqueueWatchOperation(watched, func() error {
		attempts++
		if attempts == 1 {
			return errors.New("temporary exporter failure")
		}
		close(completed)
		return nil
	})

	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("watch operation was not retried")
	}
	if attempts != 2 {
		t.Fatalf("watch operation attempts = %d, want 2", attempts)
	}
}

type transientHandoffCR struct {
	attempts int
	cr       api.ICustomResource
	firstErr error
}

func (f *transientHandoffCR) Get(context.Context, string, string) (api.ICustomResource, error) {
	f.attempts++
	if f.attempts == 1 {
		return nil, f.firstErr
	}
	return f.cr, nil
}

func (f *transientHandoffCR) StatusUpdate(context.Context, api.ICustomResource, commonTypes.UpdateStatusOptions) error {
	return nil
}

type handoffKube struct {
	interfaces.IKube
	cr interfaces.IKubeCR
}

func (k *handoffKube) CR() interfaces.IKubeCR {
	return k.cr
}

func Test_handoffReadRetriesTransientFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "service unavailable", err: apiErrors.NewServiceUnavailable("temporary API failure")},
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stg := newCHI("chi", map[string]string{testShardKey: "stg"})
			fakeCR := &transientHandoffCR{cr: stg, firstErr: tt.err}
			w := &worker{c: &Controller{kube: &handoffKube{cr: fakeCR}}}

			got, err := w.getHandoffCHI(context.Background(), stg)
			if err != nil {
				t.Fatalf("getHandoffCHI() returned error: %v", err)
			}
			if got.GetName() != stg.GetName() {
				t.Fatalf("getHandoffCHI() returned %q, want %q", got.GetName(), stg.GetName())
			}
			if fakeCR.attempts != 2 {
				t.Fatalf("getHandoffCHI() attempts = %d, want 2", fakeCR.attempts)
			}
		})
	}
}

func Test_handoffReadDoesNotRetryTerminalFailure(t *testing.T) {
	stg := newCHI("chi", map[string]string{testShardKey: "stg"})
	fakeCR := &transientHandoffCR{
		cr:       stg,
		firstErr: apiErrors.NewUnauthorized("access denied"),
	}
	w := &worker{c: &Controller{kube: &handoffKube{cr: fakeCR}}}

	if _, err := w.getHandoffCHI(context.Background(), stg); !apiErrors.IsUnauthorized(err) {
		t.Fatalf("getHandoffCHI() error = %v, want Unauthorized", err)
	}
	if fakeCR.attempts != 1 {
		t.Fatalf("getHandoffCHI() attempts = %d, want 1", fakeCR.attempts)
	}
}

func Test_staleHandoffCommandsUseLiveOwnership(t *testing.T) {
	setWatchLabelSelector(t, testShardKey+"=stg")
	stg := newCHI("chi", map[string]string{testShardKey: "stg"})
	logs := newCHI("chi", map[string]string{testShardKey: "logs"})

	t.Run("stale unwatch", func(t *testing.T) {
		w := &worker{c: &Controller{kube: &backupSecretOwnerFakeKube{crs: map[string]*api.ClickHouseInstallation{"chi": stg}}}}
		if err := w.unwatchCHI(context.Background(), stg, logs); err != nil {
			t.Fatalf("unwatchCHI() returned error: %v", err)
		}
	})

	t.Run("stale watch", func(t *testing.T) {
		w := &worker{c: &Controller{kube: &backupSecretOwnerFakeKube{crs: map[string]*api.ClickHouseInstallation{"chi": logs}}}}
		if err := w.watchCHI(context.Background(), logs, stg); err != nil {
			t.Fatalf("watchCHI() returned error: %v", err)
		}
	})
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
