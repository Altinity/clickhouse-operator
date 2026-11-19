// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chk

import (
	"context"
	"errors"
	"sync"
	"time"

	apiExtensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/kube"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	operatorMetrics "github.com/altinity/clickhouse-operator/pkg/metrics/operator"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// raftQuorumDeferredRequeueAfter is the fixed retry interval when reconcile
// returns ErrCRUDDeferred (Raft quorum headroom not available yet).
const raftQuorumDeferredRequeueAfter = 5 * time.Second

// Controller reconciles a ClickHouseKeeper object
type Controller struct {
	client.Client
	// APIReader is a non-cached client.Reader for direct API-server reads.
	// It is used by the STS client to avoid stale-cache races in the forced-restart path.
	APIReader client.Reader
	Scheme    *apiMachinery.Scheme
	ExtClient apiExtensions.Interface

	namer            interfaces.INameManager
	kube             interfaces.IKube
	activeMu         sync.Mutex
	activeReconciles map[kubeTypes.NamespacedName]*activeReconcile
	//labeler    *Labeler
	//pvcDeleter *volume.PVCDeleter
}

type activeReconcile struct {
	cancel context.CancelFunc
}

// NewController creates a CHK Controller with its request-independent collaborators
// (name manager, kube adapter) initialized once, at construction. controller-runtime
// reuses a single Controller across all MaxConcurrentReconciles goroutines, so these
// fields must NOT be (re)assigned per-Reconcile — doing so is a data race once
// reconcileCHKsThreadsNumber > 1. The CHI controller initializes the same way.
func NewController(c client.Client, apiReader client.Reader, scheme *apiMachinery.Scheme, extClient apiExtensions.Interface) *Controller {
	namer := managers.NewNameManager(managers.NameManagerTypeKeeper)
	return &Controller{
		Client:    c,
		APIReader: apiReader,
		Scheme:    scheme,
		ExtClient: extClient,
		namer:     namer,
		kube:      kube.NewAdapter(c, apiReader, namer),
	}
}

func (c *Controller) beginReconcile(parent context.Context, key kubeTypes.NamespacedName) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	active := &activeReconcile{cancel: cancel}

	c.activeMu.Lock()
	if c.activeReconciles == nil {
		c.activeReconciles = make(map[kubeTypes.NamespacedName]*activeReconcile)
	}
	previous := c.activeReconciles[key]
	c.activeReconciles[key] = active
	c.activeMu.Unlock()
	if previous != nil {
		previous.cancel()
	}

	return ctx, func() {
		cancel()
		c.activeMu.Lock()
		if c.activeReconciles[key] == active {
			delete(c.activeReconciles, key)
		}
		c.activeMu.Unlock()
	}
}

// CancelReconcile cancels active work after a CHK leaves this controller's watch scope.
func (c *Controller) CancelReconcile(namespace, name string) {
	key := kubeTypes.NamespacedName{Namespace: namespace, Name: name}
	c.activeMu.Lock()
	active := c.activeReconciles[key]
	c.activeMu.Unlock()
	if active != nil {
		active.cancel()
	}
}

func (c *Controller) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return ctrl.Result{}, nil
	}
	ctx, finish := c.beginReconcile(ctx, req.NamespacedName)
	defer finish()

	// Guard: skip namespaces outside the configured watch list.
	// keeperPredicate() filters direct CHK events, but reconcile requests triggered by owned
	// StatefulSet changes bypass it. This guard catches all paths including those.
	if !chop.Config().IsNamespaceWatched(req.Namespace) {
		log.V(2).Info("skip reconcile, namespace '%s' is not watched", req.Namespace)
		return ctrl.Result{}, nil
	}

	// Fetch the ClickHouseKeeper instance
	new := &apiChk.ClickHouseKeeperInstallation{}
	if err := c.Client.Get(ctx, req.NamespacedName, new); err != nil {
		if apiErrors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			// Return and don't requeue
			return ctrl.Result{}, nil
		}
		// Return and requeue
		return ctrl.Result{}, err
	}

	// Label guard here, not just in the predicate: reconcile requests from owned
	// StatefulSet changes bypass predicates.
	if !chop.Config().IsLabelSelectorWatched(new.GetLabels()) {
		log.V(2).M(new).Info("skip reconcile, CHK labels do not match watch.labelSelector '%s'", chop.Config().Watch.LabelSelector)
		return ctrl.Result{}, nil
	}

	// The cached read above can lag a shard-label flip; confirm ownership on live state
	// before any write (finalizer install, child-object reconcile, deletion protocol).
	if chop.Config().HasWatchLabelSelector() {
		owned, err := c.ownsLiveCR(ctx, req.Namespace, req.Name)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !owned {
			log.V(1).M(new).Info("skip reconcile, live CHK ownership not confirmed (shard label flipped?): %s/%s", req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
	}

	w := c.newWorker()

	if w.ensureFinalizer(ctx, new) {
		// Finalizer just installed; controller-runtime will re-reconcile
		return ctrl.Result{}, nil
	}
	if util.IsContextDone(ctx) {
		return ctrl.Result{}, nil
	}

	if w.deleteCHK(ctx, new) {
		// CHK is being deleted
		return ctrl.Result{}, nil
	}
	if util.IsContextDone(ctx) {
		return ctrl.Result{}, nil
	}

	if new.Spec.Suspend.Value() {
		log.V(2).M(new).F().Info("CR is suspended, skip reconcile")
		return ctrl.Result{}, nil
	}

	return c.reconcileResult(new, w.reconcileCR(ctx, nil, new))
}

// reconcileResult maps the outcome of reconcileCR onto a controller-runtime result.
//
// ErrCRUDDeferred is an intentional postponement, not a failure, so it must not be
// returned as an error: controller-runtime would apply exponential backoff to a wait
// that is expected to clear within seconds. Match with errors.Is - the reconciler
// wraps the sentinel with host context on its way up.
func (c *Controller) reconcileResult(cr *apiChk.ClickHouseKeeperInstallation, err error) (ctrl.Result, error) {
	switch {
	case err == nil:
		return ctrl.Result{}, nil
	case errors.Is(err, common.ErrCRUDDeferred):
		// Soft defer: quorum headroom not available yet. Status already
		// records [RaftQuorumUnsafe]; retry on a fixed interval instead of
		// error backoff.
		log.V(1).M(cr).F().Info(
			"Raft quorum defer — requeue in %s", raftQuorumDeferredRequeueAfter,
		)
		return ctrl.Result{RequeueAfter: raftQuorumDeferredRequeueAfter}, nil
	default:
		return ctrl.Result{}, err
	}
}

// installFinalizer adds the operator finalizer to the CHK CR.
func (c *Controller) installFinalizer(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation) error {
	cur := &apiChk.ClickHouseKeeperInstallation{}
	if err := c.Client.Get(ctx, kubeTypes.NamespacedName{Namespace: chk.Namespace, Name: chk.Name}, cur); err != nil {
		return err
	}
	if util.InArray(FinalizerName, cur.GetFinalizers()) {
		return nil
	}
	base := cur.DeepCopy()
	cur.SetFinalizers(append(cur.GetFinalizers(), FinalizerName))
	return c.Client.Patch(ctx, cur, client.MergeFrom(base))
}

// uninstallFinalizer removes the operator finalizer from the CHK CR, allowing k8s to delete it.
func (c *Controller) uninstallFinalizer(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation) error {
	cur := &apiChk.ClickHouseKeeperInstallation{}
	if err := c.Client.Get(ctx, kubeTypes.NamespacedName{Namespace: chk.Namespace, Name: chk.Name}, cur); err != nil {
		return err
	}
	base := cur.DeepCopy()
	cur.SetFinalizers(util.RemoveFromArray(FinalizerName, cur.GetFinalizers()))
	return c.Client.Patch(ctx, cur, client.MergeFrom(base))
}

func (c *Controller) poll(ctx context.Context, cr api.ICustomResource, f func(c *apiChk.ClickHouseKeeperInstallation, e error) bool) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Poll is aborted before start. CR: %s", cr.GetName())
		return
	}

	namespace, name := util.NamespaceName(cr)

	for {
		cur, err := c.kube.CR().Get(ctx, namespace, name)
		chk, ok := cur.(*apiChk.ClickHouseKeeperInstallation)
		if !ok || (chk == nil) {
			// Fetched object is not good, handle error
			switch {
			case apiErrors.IsNotFound(err):
				// Object not found, not an error per se, just nothing to poll
				return
			case err != nil:
				// Other non-not-found error, go to next poll cycle, retry
				log.Warning("poll Get error for %s: %v", cr.GetName(), err)
			default:
				// err is nil, but we have issue with chk, go to next poll cycle, retry
				log.V(1).Info("poll Get returned unexpected type for %s: %T", cr.GetName(), cur)
			}

			// Go to next poll cycle, retry

			if util.WaitContextDoneOrTimeout(ctx, 15*time.Second) {
				log.V(1).Info("Poll is aborted before retry. CR: %s", cr.GetName())
				return
			}
			continue
		}

		// Fetched object is good, call function to process it

		if f(chk, err) {
			// Function says to continue polling
			if util.WaitContextDoneOrTimeout(ctx, 15*time.Second) {
				log.V(1).Info("Poll is aborted after polling fn. CR: %s", cr.GetName())
				return
			}
		} else {
			// Function says to stop polling
			return
		}
	}
}

// ownsLiveCR re-checks watch scope against live (non-cached) CR state — the entry guard in
// Reconcile reads the informer cache, which can miss a shard label flip mid-reconcile.
// NotFound is not-owned; transient read failures are returned so reconciliation retries.
func (c *Controller) ownsLiveCR(ctx context.Context, namespace, name string) (bool, error) {
	live := &apiChk.ClickHouseKeeperInstallation{}
	if err := c.APIReader.Get(ctx, kubeTypes.NamespacedName{Namespace: namespace, Name: name}, live); err != nil {
		if apiErrors.IsNotFound(err) {
			return false, nil
		}
		log.V(1).Warning("unable to confirm live CHK ownership %s/%s: %v", namespace, name, err)
		return false, err
	}
	return chop.Config().IsCRWatched(live.GetNamespace(), live.GetLabels()), nil
}

func ShouldEnqueue(cr *apiChk.ClickHouseKeeperInstallation) bool {
	ns := cr.GetNamespace()
	if !chop.Config().IsNamespaceWatched(ns) {
		log.V(2).M(cr).Info("skip enqueue, namespace '%s' is not watched or is in deny list", ns)
		return false
	}

	if !chop.Config().IsLabelSelectorWatched(cr.GetLabels()) {
		log.V(2).M(cr).Info("skip enqueue, CHK labels do not match watch.labelSelector '%s'", chop.Config().Watch.LabelSelector)
		operatorMetrics.CRSkippedByLabelSelector("chk", cr.GetNamespace(), cr.GetName())
		return false
	}

	return true
}
